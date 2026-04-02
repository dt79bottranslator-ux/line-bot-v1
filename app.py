import os
import json
from datetime import datetime, timezone
from typing import Any, List, Optional

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

# =========================================================
# APP INIT
# =========================================================
app = Flask(__name__)
APP_VERSION = "DT79_V5_FINAL_STABLE_DEBUG_UID"

# =========================================================
# ENV CONFIG
# =========================================================
LINE_ACCESS_TOKEN = (os.getenv("LINE_CHANNEL_ACCESS_TOKEN") or "").strip()
LINE_SECRET = (os.getenv("LINE_CHANNEL_SECRET") or "").strip()
SHEET_ID = (os.getenv("GOOGLE_SHEET_ID") or os.getenv("SPREADSHEET_ID") or "").strip()
GOOGLE_JSON = (
    os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON")
    or os.getenv("GOOGLE_CREDENTIALS_JSON")
    or ""
).strip()

SHEET_NAME = "USER_LANG_MAP"

# =========================================================
# ADMIN CONFIG
# =========================================================
def normalize_id(val: Any) -> str:
    return (
        str(val or "")
        .strip()
        .replace("\u200b", "")
        .replace("\ufeff", "")
        .replace("\u2060", "")
        .replace("\xa0", " ")
        .replace("\n", "")
        .replace("\r", "")
    )


def get_admins() -> List[str]:
    raw_admins = os.getenv("ADMIN_LIST") or "U83c6ce008a35ef17edaff25ac003370"
    admins = [normalize_id(x) for x in raw_admins.split(",") if normalize_id(x)]
    return admins


ADMIN_LIST = get_admins()

# =========================================================
# LINE SDK INIT
# =========================================================
configuration = Configuration(access_token=LINE_ACCESS_TOKEN)
handler = WebhookHandler(LINE_SECRET)

# =========================================================
# BOOT LOG
# =========================================================
print(f"[BOOT] {APP_VERSION} starting...")
print(f"[BOOT] LINE_ACCESS_TOKEN exists: {bool(LINE_ACCESS_TOKEN)}")
print(f"[BOOT] LINE_SECRET exists: {bool(LINE_SECRET)}")
print(f"[BOOT] SHEET_ID exists: {bool(SHEET_ID)}")
print(f"[BOOT] GOOGLE_JSON exists: {bool(GOOGLE_JSON)}")
print(f"[BOOT] Validated Admins: {ADMIN_LIST}")

# =========================================================
# REPLY
# =========================================================
def reply_msg(token: str, text: str):
    try:
        with ApiClient(configuration) as api_client:
            api_instance = MessagingApi(api_client)
            api_instance.reply_message(
                ReplyMessageRequest(
                    reply_token=token,
                    messages=[V3TextMessage(text=text)]
                )
            )
        print(f"[REPLY] {repr(text)}")
    except Exception as e:
        print(f"[REPLY ERROR] {str(e)}")


# =========================================================
# GOOGLE SHEET
# =========================================================
def get_ws():
    try:
        creds = ServiceAccountCredentials.from_json_keyfile_dict(
            json.loads(GOOGLE_JSON),
            [
                "https://spreadsheets.google.com/feeds",
                "https://www.googleapis.com/auth/drive",
            ],
        )
        ws = gspread.authorize(creds).open_by_key(SHEET_ID).worksheet(SHEET_NAME)
        return ws
    except Exception as e:
        print(f"[SHEET ERROR] {e}")
        return None


def find_user_row_index(ws, target_uid: str) -> Optional[int]:
    if ws is None:
        print("[FIND USER] worksheet=None")
        return None

    target = normalize_id(target_uid)

    try:
        all_rows = ws.get_all_values()
        print(f"[FIND USER] target={repr(target)} total_rows={len(all_rows)}")

        for i, row in enumerate(all_rows):
            if i == 0:
                continue

            raw_sheet_uid = row[0] if len(row) > 0 else ""
            sheet_uid = normalize_id(raw_sheet_uid)

            print(
                f"[COMPARE] row={i + 1} "
                f"sheet_raw={repr(raw_sheet_uid)} "
                f"sheet_norm={repr(sheet_uid)} "
                f"target={repr(target)}"
            )

            if sheet_uid == target:
                print(f"[MATCH FOUND] row_index={i + 1}")
                return i + 1

        print("[MATCH FAILED]")
        return None

    except Exception as e:
        print(f"[FIND USER ERROR] {str(e)}")
        return None


def set_user_premium(target_uid: str) -> bool:
    ws = get_ws()
    if not ws:
        print("[PREMIUM SET] worksheet unavailable")
        return False

    target = normalize_id(target_uid)

    try:
        row_idx = find_user_row_index(ws, target)

        if row_idx:
            print(f"[PREMIUM SET] existing row={row_idx}, updating premium=TRUE")
            try:
                ws.update_cell(row_idx, 4, "TRUE")  # cột D = is_premium
                ws.update_cell(row_idx, 3, datetime.now(timezone.utc).isoformat())  # cột C = updated_at
                print(f"[PREMIUM SET] success existing user_id={target}")
                return True
            except Exception as write_err:
                print(f"[SHEET WRITE ERROR] existing row update failed: {str(write_err)}")
                return False

        print(f"[PREMIUM SET] user not found, append new row for user_id={target}")
        try:
            ws.append_row([
                target,
                "en",
                datetime.now(timezone.utc).isoformat(),
                "TRUE",
                "0",
                "USER",
                "user",
            ])
            print(f"[PREMIUM SET] success appended user_id={target}")
            return True
        except Exception as append_err:
            print(f"[SHEET WRITE ERROR] append failed: {str(append_err)}")
            return False

    except Exception as e:
        print(f"[PREMIUM SET ERROR] {str(e)}")
        return False


# =========================================================
# ROUTES
# =========================================================
@app.route("/", methods=["GET"])
def home():
    return f"{APP_VERSION} is LIVE", 200


@app.route("/webhook", methods=["POST"])
def callback():
    sig = request.headers.get("X-Line-Signature", "")
    body = request.get_data(as_text=True)

    print(f"[WEBHOOK] signature_exists={bool(sig)}")
    print(f"[WEBHOOK] body={body}")

    try:
        handler.handle(body, sig)
    except InvalidSignatureError:
        print("[WEBHOOK ERROR] InvalidSignatureError")
        abort(400)
    except Exception as e:
        print(f"[WEBHOOK ERROR] {str(e)}")
        abort(500)

    return "OK"


# =========================================================
# MESSAGE HANDLER
# =========================================================
@handler.add(MessageEvent, message=TextMessageContent)
def handle_text(event):
    real_uid = normalize_id(event.source.user_id)
    text = (event.message.text or "").strip()
    token = event.reply_token

    print(f"[REAL USER ID] {real_uid}")
    print(f"[INCOMING] UID={real_uid} | Text={repr(text)}")

    # CHẶN COMMAND Ở TẦNG CAO NHẤT
    if text.startswith("/"):
        if real_uid not in ADMIN_LIST:
            print(f"[AUTH DENIED] {real_uid} is not in {ADMIN_LIST}")
            return reply_msg(token, f"❌ Bạn không có quyền Admin.\nID của bạn: {real_uid}")

        print(f"[ADMIN PASS] {real_uid} is in ADMIN_LIST")

        parts = text.split()
        print(f"[COMMAND] parts={parts}")

        if parts[0].lower() == "/grant":
            if len(parts) != 2:
                print("[COMMAND ERROR] syntax_error /grant")
                return reply_msg(token, "Cú pháp: /grant USER_ID")

            target = normalize_id(parts[1])
            print(f"[TARGET_UID] {target}")

            success = set_user_premium(target)

            if success:
                return reply_msg(token, f"✅ Đã cấp Premium cho {target}")
            return reply_msg(token, "❌ Cấp premium thất bại")

        if parts[0].lower() == "/revoke":
            if len(parts) != 2:
                print("[COMMAND ERROR] syntax_error /revoke")
                return reply_msg(token, "Cú pháp: /revoke USER_ID")

            target = normalize_id(parts[1])
            ws = get_ws()
            if not ws:
                return reply_msg(token, "❌ Lỗi kết nối Google Sheet.")

            try:
                row_idx = find_user_row_index(ws, target)
                if not row_idx:
                    return reply_msg(token, f"❌ Không tìm thấy user: {target}")

                ws.update_cell(row_idx, 4, "FALSE")
                ws.update_cell(row_idx, 3, datetime.now(timezone.utc).isoformat())
                print(f"[PREMIUM REVOKE] success user_id={target}")
                return reply_msg(token, f"🚫 Đã gỡ Premium cho {target}")
            except Exception as e:
                print(f"[PREMIUM REVOKE ERROR] {str(e)}")
                return reply_msg(token, f"❌ Lỗi: {str(e)}")

        print("[COMMAND] unsupported command")
        return reply_msg(token, "Lệnh không hỗ trợ.")

    # KHÔNG PHẢI COMMAND → KHÔNG DỊCH, CHỈ LOG
    print(f"[NORMAL MESSAGE] {repr(text)}")
    return reply_msg(token, f"[DEBUG] UID của bạn: {real_uid}")


# =========================================================
# MAIN
# =========================================================
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 10000)))
