import os
import json
import re
import uuid
import hashlib
from datetime import datetime, timezone
from typing import List, Tuple, Optional

import gspread
from flask import Flask, request, abort
from oauth2client.service_account import ServiceAccountCredentials
from linebot.v3.messaging import (
    Configuration,
    ApiClient,
    MessagingApi,
    ReplyMessageRequest,
    TextMessage as V3TextMessage,
)
from linebot.v3.webhook import WebhookHandler
from linebot.v3.exceptions import InvalidSignatureError
from linebot.v3.webhooks import MessageEvent, TextMessageContent

app = Flask(__name__)
APP_VERSION = "DT79_V16_EVENT_TRUTH_LOCK"

# =========================================================
# FAIL-FAST ENV (chặn app nửa sống nửa chết)
# =========================================================
def get_required_env(name: str) -> str:
    value = os.getenv(name)
    if not value or not value.strip():
        raise RuntimeError(f"CRITICAL: Missing ENV {name}")
    return value.strip()

try:
    LINE_ACCESS_TOKEN = get_required_env("LINE_CHANNEL_ACCESS_TOKEN")
    LINE_SECRET = get_required_env("LINE_CHANNEL_SECRET")
    SHEET_ID = get_required_env("GOOGLE_SHEET_ID")
    GOOGLE_JSON = get_required_env("GOOGLE_SERVICE_ACCOUNT_JSON")
except RuntimeError as e:
    print(e)
    raise SystemExit(1)

# =========================================================
# CONFIG
# =========================================================
ADMIN_ID = "U83c6ce008a35ef17edaff25ac003370"

USER_SHEET = "USER_LANG_MAP"
EVENT_SHEET = "ACCESS_EVENTS"

SCHEMA_CONFIG = {
    USER_SHEET: {
        "header": ["UID", "Language", "Timestamp", "Premium", "Count", "Role", "Note"],
        "sentinel_row": ["DT79_USER_LANG_MAP_LOCK", "", "", "", "", "", ""],
    },
    EVENT_SHEET: {
        "header": [
            "EVENT_ID",
            "TARGET_UID",
            "ACTION",
            "BY_ADMIN_UID",
            "EVENT_TS",
            "NOTE",
            "PAYLOAD_JSON",
            "CHECKSUM",
            "RESULT_STATUS",
            "RESULT_MESSAGE",
        ],
        "sentinel_row": ["DT79_ACCESS_EVENTS_LOCK", "", "", "", "", "", "", "", "", ""],
    },
}

configuration = Configuration(access_token=LINE_ACCESS_TOKEN)
handler = WebhookHandler(LINE_SECRET)

# =========================================================
# HELPERS
# =========================================================
def normalize_id(val: object) -> str:
    s = str(val or "").strip()
    return re.sub(r"[\s\n\r\t\u200b\ufeff]", "", s)

def is_valid_line_uid(uid: str) -> bool:
    # UID LINE user thường bắt đầu bằng U + 32 ký tự hex (tổng 33)
    return bool(re.fullmatch(r"U[0-9a-f]{32}", uid))

def make_checksum(values: List[str]) -> str:
    raw = "|".join(values)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:20]

def get_gspread_client():
    creds = ServiceAccountCredentials.from_json_keyfile_dict(
        json.loads(GOOGLE_JSON),
        [
            "https://spreadsheets.google.com/feeds",
            "https://www.googleapis.com/auth/drive",
        ],
    )
    return gspread.authorize(creds)

def get_ws(sheet_name: str):
    try:
        client = get_gspread_client()
        return client.open_by_key(SHEET_ID).worksheet(sheet_name)
    except Exception as e:
        print(f"[SHEET ACCESS ERROR] {sheet_name}: {e}")
        return None

def reply_msg(token: str, text: str):
    with ApiClient(configuration) as api_client:
        MessagingApi(api_client).reply_message(
            ReplyMessageRequest(
                reply_token=token,
                messages=[V3TextMessage(text=text)]
            )
        )

def stable_row_hash(values: List[str]) -> str:
    normalized = [str(v or "") for v in values]
    return hashlib.sha256("|".join(normalized).encode("utf-8")).hexdigest()

def check_schema_lock(ws, sheet_name: str) -> Tuple[bool, str]:
    """
    Kiểm tra:
    1. Header đúng tuyệt đối
    2. Sentinel row đúng tuyệt đối
    3. Fingerprint (dấu vân tay) đúng
    """
    expected_header = SCHEMA_CONFIG[sheet_name]["header"]
    expected_sentinel = SCHEMA_CONFIG[sheet_name]["sentinel_row"]
    width = len(expected_header)

    try:
        actual_header = ws.row_values(1)
        actual_sentinel = ws.row_values(2)

        actual_header = (actual_header + [""] * width)[:width]
        actual_sentinel = (actual_sentinel + [""] * width)[:width]

        if actual_header != expected_header:
            return False, f"{sheet_name}: HEADER_MISMATCH"

        if actual_sentinel != expected_sentinel:
            return False, f"{sheet_name}: SENTINEL_MISMATCH"

        expected_fp = stable_row_hash(expected_header + expected_sentinel)
        actual_fp = stable_row_hash(actual_header + actual_sentinel)

        if expected_fp != actual_fp:
            return False, f"{sheet_name}: FINGERPRINT_MISMATCH"

        return True, "OK"

    except Exception as e:
        return False, f"{sheet_name}: SCHEMA_CHECK_ERROR: {str(e)}"

def find_uid_rows(ws_user, target_uid: str) -> List[int]:
    """
    Chỉ tìm trong cột UID (cột A), bỏ qua dòng 1 và 2
    """
    col_uids = ws_user.col_values(1)
    rows = []

    for row_index, cell_value in enumerate(col_uids, start=1):
        if row_index <= 2:
            continue
        if normalize_id(cell_value) == target_uid:
            rows.append(row_index)

    return rows

def append_pending_event(
    ws_event,
    event_id: str,
    target_uid: str,
    admin_uid: str,
    event_ts: str,
    payload_json: str,
) -> Tuple[bool, str, int]:
    """
    Ghi event PENDING trước để luôn có dấu vết nếu fail giữa chừng.
    Trả về:
    - success
    - message
    - row index của event
    """
    try:
        pending_checksum = make_checksum([
            event_id,
            target_uid,
            "grant_premium",
            admin_uid,
            event_ts,
            "PENDING",
            "Processing",
        ])

        row = [
            event_id,                # EVENT_ID
            target_uid,              # TARGET_UID
            "grant_premium",         # ACTION
            admin_uid,               # BY_ADMIN_UID
            event_ts,                # EVENT_TS
            "Grant command accepted, processing",  # NOTE
            payload_json,            # PAYLOAD_JSON
            pending_checksum,        # CHECKSUM
            "PENDING",               # RESULT_STATUS
            "Processing",            # RESULT_MESSAGE
        ]
        ws_event.append_row(row)

        event_ids = ws_event.col_values(1)
        matched_rows = [i for i, v in enumerate(event_ids, start=1) if v == event_id]

        if not matched_rows:
            return False, "EVENT_ROW_NOT_FOUND_AFTER_APPEND", -1

        return True, "OK", matched_rows[-1]

    except Exception as e:
        return False, f"EVENT_APPEND_FAILED: {str(e)}", -1

def finalize_event(
    ws_event,
    event_row: int,
    event_id: str,
    target_uid: str,
    admin_uid: str,
    event_ts: str,
    result_status: str,
    result_message: str,
) -> bool:
    """
    Chốt event từ PENDING -> SUCCESS/FAILED
    """
    try:
        final_checksum = make_checksum([
            event_id,
            target_uid,
            "grant_premium",
            admin_uid,
            event_ts,
            result_status,
            result_message,
        ])

        ws_event.update_cell(event_row, 6, f"Finalized by bot at {datetime.now(timezone.utc).isoformat()}")
        ws_event.update_cell(event_row, 8, final_checksum)
        ws_event.update_cell(event_row, 9, result_status)
        ws_event.update_cell(event_row, 10, result_message)
        return True

    except Exception as e:
        print(f"[EVENT FINALIZE ERROR] row={event_row}: {e}")
        return False

def apply_user_grant(ws_user, target_uid: str, event_ts: str) -> Tuple[bool, str]:
    """
    Trả về:
    - True/False
    - message kết quả
    """
    rows = find_uid_rows(ws_user, target_uid)

    if len(rows) > 1:
        return False, f"DUPLICATE_UID_ROWS: {rows}"

    try:
        if len(rows) == 1:
            row_num = rows[0]
            ws_user.update_cell(row_num, 3, event_ts)      # Timestamp
            ws_user.update_cell(row_num, 4, "TRUE")        # Premium
            return True, f"UPDATED_ROW_{row_num}"

        ws_user.append_row([target_uid, "en", event_ts, "TRUE", "0", "USER", "Grant by Bot"])
        return True, "CREATED_NEW_ROW"

    except Exception as e:
        return False, f"USER_MAP_WRITE_FAILED: {str(e)}"

# =========================================================
# ROUTES
# =========================================================
@app.route("/", methods=["GET"])
def home():
    return f"{APP_VERSION} LIVE", 200

@app.route("/webhook", methods=["POST"])
def callback():
    signature = request.headers.get("X-Line-Signature", "")
    body = request.get_data(as_text=True)

    try:
        handler.handle(body, signature)
    except InvalidSignatureError:
        abort(400)

    return "OK"

# =========================================================
# MAIN HANDLER
# =========================================================
@handler.add(MessageEvent, message=TextMessageContent)
def handle_text(event):
    runtime_uid = normalize_id(event.source.user_id)
    reply_token = event.reply_token
    raw_text = event.message.text or ""
    clean_text = raw_text.strip()
    clean_lower = clean_text.lower()

    if clean_lower == "/me":
        reply_msg(reply_token, f"ID của bạn:\n{runtime_uid}")
        return

    if not clean_lower.startswith("/grant"):
        return

    # AUTH CHECK
    if runtime_uid != normalize_id(ADMIN_ID):
        reply_msg(reply_token, "❌ Quyền Admin bị từ chối.")
        return

    parts = clean_text.split()
    if len(parts) < 2:
        reply_msg(reply_token, "HD: /grant USER_ID")
        return

    target_uid = normalize_id(parts[1])

    if not is_valid_line_uid(target_uid):
        reply_msg(reply_token, f"❌ UID mục tiêu không hợp lệ:\n|{target_uid}|")
        return

    ws_event = get_ws(EVENT_SHEET)
    ws_user = get_ws(USER_SHEET)

    if not ws_event or not ws_user:
        reply_msg(reply_token, "❌ Lỗi kết nối Google Sheet.")
        return

    # 1) Event sheet phải sạch trước vì đây là hộp đen audit
    ok_event_schema, event_schema_msg = check_schema_lock(ws_event, EVENT_SHEET)
    if not ok_event_schema:
        reply_msg(reply_token, f"❌ SCHEMA DRIFT DETECTED:\n{event_schema_msg}\nDừng ghi để bảo vệ dữ liệu.")
        return

    event_id = str(uuid.uuid4())[:8]
    event_ts = datetime.now(timezone.utc).isoformat()
    payload_json = json.dumps(
        {"target": target_uid, "version": APP_VERSION},
        ensure_ascii=False
    )

    # 2) Append PENDING trước
    ok_pending, pending_msg, event_row = append_pending_event(
        ws_event=ws_event,
        event_id=event_id,
        target_uid=target_uid,
        admin_uid=runtime_uid,
        event_ts=event_ts,
        payload_json=payload_json,
    )

    if not ok_pending:
        reply_msg(reply_token, f"❌ EVENT LOG ERROR:\n{pending_msg}")
        return

    # 3) Từ đây mọi lỗi phải cố finalize FAILED nếu có thể
    ok_user_schema, user_schema_msg = check_schema_lock(ws_user, USER_SHEET)
    if not ok_user_schema:
        finalize_event(
            ws_event=ws_event,
            event_row=event_row,
            event_id=event_id,
            target_uid=target_uid,
            admin_uid=runtime_uid,
            event_ts=event_ts,
            result_status="FAILED",
            result_message=user_schema_msg,
        )
        reply_msg(reply_token, f"❌ SCHEMA DRIFT DETECTED:\n{user_schema_msg}\nDừng ghi để bảo vệ dữ liệu.")
        return

    success, result_message = apply_user_grant(
        ws_user=ws_user,
        target_uid=target_uid,
        event_ts=event_ts,
    )

    final_status = "SUCCESS" if success else "FAILED"

    finalize_ok = finalize_event(
        ws_event=ws_event,
        event_row=event_row,
        event_id=event_id,
        target_uid=target_uid,
        admin_uid=runtime_uid,
        event_ts=event_ts,
        result_status=final_status,
        result_message=result_message,
    )

    if success:
        if finalize_ok:
            reply_msg(reply_token, f"✅ SUCCESS\nTarget: {target_uid}\nLog ID: {event_id}")
        else:
            reply_msg(reply_token, f"⚠️ STATE OK nhưng EVENT FINALIZE lỗi.\nTarget: {target_uid}\nLog ID: {event_id}")
    else:
        if finalize_ok:
            reply_msg(reply_token, f"❌ FAILED\n{result_message}\nLog ID: {event_id}")
        else:
            reply_msg(reply_token, f"❌ FAILED + EVENT FINALIZE ERROR\n{result_message}\nLog ID: {event_id}")

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 10000)))
