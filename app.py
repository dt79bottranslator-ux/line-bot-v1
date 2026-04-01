import os
import json
import hmac
import base64
import hashlib
from datetime import datetime, timezone
from typing import Any, Optional, List

import gspread
from flask import Flask, request, abort
from oauth2client.service_account import ServiceAccountCredentials
from linebot import LineBotApi, WebhookHandler
from linebot.exceptions import InvalidSignatureError
from linebot.models import MessageEvent, TextMessage, TextSendMessage

# =========================================================
# APP INIT
# =========================================================
app = Flask(__name__)

# =========================================================
# ENVIRONMENT VARIABLES
# =========================================================
LINE_CHANNEL_ACCESS_TOKEN = (os.getenv("LINE_CHANNEL_ACCESS_TOKEN") or "").strip()
LINE_CHANNEL_SECRET = (os.getenv("LINE_CHANNEL_SECRET") or "").strip()
GOOGLE_SHEET_ID = (os.getenv("GOOGLE_SHEET_ID") or "").strip()
GOOGLE_CREDENTIALS_JSON = (
    os.getenv("GOOGLE_CREDENTIALS_JSON")
    or os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON")
    or ""
).strip()

# Admin UID:
# Ưu tiên đọc từ ADMIN_LIST trong env, ngăn cách bằng dấu phẩy.
# Nếu chưa set env, tạm dùng UID admin hiện tại của bạn.
DEFAULT_ADMIN_UID = "U83c6ce008a35ef17edaff25ac003370"
ADMIN_LIST_RAW = (os.getenv("ADMIN_LIST") or DEFAULT_ADMIN_UID).strip()

USER_LANG_SHEET_NAME = "USER_LANG_MAP"

COL_USER_ID = 0
COL_TARGET_LANG = 1
COL_UPDATED_AT = 2
COL_IS_PREMIUM = 3
COL_USAGE_COUNT = 4
COL_GROUP_ID = 5
COL_ROLE = 6

line_bot_api = LineBotApi(LINE_CHANNEL_ACCESS_TOKEN)
handler = WebhookHandler(LINE_CHANNEL_SECRET)

# =========================================================
# BOOT LOG
# =========================================================
print("[BOOT] Starting LINE bot")
print(f"[BOOT] LINE_CHANNEL_ACCESS_TOKEN exists: {bool(LINE_CHANNEL_ACCESS_TOKEN)}")
print(f"[BOOT] LINE_CHANNEL_SECRET exists: {bool(LINE_CHANNEL_SECRET)}")
print(f"[BOOT] GOOGLE_SHEET_ID exists: {bool(GOOGLE_SHEET_ID)}")
print(f"[BOOT] GOOGLE_CREDENTIALS_JSON exists: {bool(GOOGLE_CREDENTIALS_JSON)}")
print(f"[BOOT] ADMIN_LIST_RAW={ADMIN_LIST_RAW}")

# =========================================================
# UTILS
# =========================================================
def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def normalize_id(value: Any) -> str:
    if value is None:
        return ""
    text = str(value)
    text = text.replace("\u200b", "")
    text = text.replace("\ufeff", "")
    text = text.replace("\u2060", "")
    text = text.replace("\xa0", " ")
    text = text.replace("\n", "")
    text = text.replace("\r", "")
    return text.strip()

def get_admin_list() -> List[str]:
    return [normalize_id(x) for x in ADMIN_LIST_RAW.split(",") if normalize_id(x)]

# =========================================================
# GOOGLE SHEET
# =========================================================
def get_gspread_client():
    if not GOOGLE_CREDENTIALS_JSON:
        print("[SHEET ERROR] GOOGLE_CREDENTIALS_JSON missing")
        return None

    try:
        credentials_dict = json.loads(GOOGLE_CREDENTIALS_JSON)
        scope = [
            "https://spreadsheets.google.com/feeds",
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ]
        credentials = ServiceAccountCredentials.from_json_keyfile_dict(
            credentials_dict,
            scope,
        )
        return gspread.authorize(credentials)
    except Exception as exc:
        print(f"[SHEET ERROR] authorize failed: {str(exc)}")
        return None

def get_worksheet(name: str):
    if not GOOGLE_SHEET_ID:
        print("[SHEET ERROR] GOOGLE_SHEET_ID missing")
        return None

    client = get_gspread_client()
    if client is None:
        return None

    try:
        spreadsheet = client.open_by_key(GOOGLE_SHEET_ID)
        return spreadsheet.worksheet(name)
    except Exception as exc:
        print(f"[SHEET ERROR] open worksheet failed: {str(exc)}")
        return None

def find_user_row_index(worksheet, user_id: str) -> Optional[int]:
    target = normalize_id(user_id)

    if worksheet is None:
        print("[FIND USER] worksheet=None")
        return None

    try:
        values = worksheet.get_all_values()
        print(f"[FIND USER] target={repr(target)} total_rows={len(values)}")

        for idx, row in enumerate(values):
            if idx == 0:
                continue

            raw_sheet_id = row[COL_USER_ID] if len(row) > COL_USER_ID else ""
            row_user_id = normalize_id(raw_sheet_id)

            print(
                f"[COMPARE] row={idx + 1} "
                f"sheet_raw={repr(raw_sheet_id)} "
                f"sheet_norm={repr(row_user_id)} "
                f"target={repr(target)}"
            )

            if row_user_id == target:
                print(f"[MATCH FOUND] row_index={idx + 1}")
                return idx + 1

        print("[MATCH FAILED]")
        return None

    except Exception as exc:
        print(f"[FIND USER ERROR] {str(exc)}")
        return None

def set_user_premium(target_uid: str, status: bool = True) -> bool:
    print(f"[PREMIUM SET] target_uid={repr(normalize_id(target_uid))} status={status}")

    ws = get_worksheet(USER_LANG_SHEET_NAME)
    if ws is None:
        print("[PREMIUM SET] worksheet unavailable")
        return False

    try:
        row_index = find_user_row_index(ws, target_uid)
        if not row_index:
            print(f"[PREMIUM SET] user_id not found: {normalize_id(target_uid)}")
            return False

        premium_text = "TRUE" if status else "FALSE"

        ws.update_cell(row_index, COL_IS_PREMIUM + 1, premium_text)
        ws.update_cell(row_index, COL_UPDATED_AT + 1, now_iso())

        print(
            f"[PREMIUM SET] row={row_index} "
            f"user_id={normalize_id(target_uid)} "
            f"premium={premium_text}"
        )
        return True

    except Exception as exc:
        print(f"[PREMIUM SET ERROR] {str(exc)}")
        return False

# =========================================================
# LINE REPLY
# =========================================================
def reply_text(reply_token: str, text: str):
    try:
        line_bot_api.reply_message(
            reply_token,
            TextSendMessage(text=text),
        )
    except Exception as exc:
        print(f"[LINE REPLY ERROR] {str(exc)}")

# =========================================================
# WEBHOOK
# =========================================================
@app.route("/", methods=["GET"])
def home():
    return "DT79 LINE BOT LIVE", 200

@app.route("/webhook", methods=["POST"])
def callback():
    signature = request.headers.get("X-Line-Signature", "")
    body = request.get_data(as_text=True)

    print(f"[WEBHOOK] body={body}")
    print(f"[WEBHOOK] signature_exists={bool(signature)}")

    try:
        handler.handle(body, signature)
    except InvalidSignatureError:
        print("[WEBHOOK ERROR] Invalid signature")
        abort(400)
    except Exception as exc:
        print(f"[WEBHOOK ERROR] {str(exc)}")
        abort(500)

    return "OK", 200

# =========================================================
# COMMAND HANDLER
# =========================================================
def process_commands(event) -> bool:
    user_id = normalize_id(event.source.user_id)
    text = (event.message.text or "").strip()
    reply_token = event.reply_token

    if not text.startswith("/"):
        return False

    print(f"[EVENT] command={repr(text)} user_id={repr(user_id)}")

    admin_list = get_admin_list()
    is_admin = user_id in admin_list

    print(f"[ROLE DEBUG] user_id={repr(user_id)} admin_list={admin_list}")
    print(f"[ADMIN] user_id={repr(user_id)} admin={is_admin}")

    if text.startswith("/grant"):
        if not is_admin:
            reply_text(reply_token, "Bạn không có quyền admin.")
            return True

        parts = text.split()
        print(f"[COMMAND] /grant parts={parts}")

        if len(parts) != 2:
            reply_text(reply_token, "Cú pháp: /grant USER_ID")
            return True

        target_uid = normalize_id(parts[1])
        print(f"[COMMAND] grant target_uid={repr(target_uid)}")

        success = set_user_premium(target_uid, True)

        if success:
            reply_text(reply_token, f"Đã cấp premium cho {target_uid}")
        else:
            reply_text(reply_token, "Cấp premium thất bại")

        return True

    if text.startswith("/revoke"):
        if not is_admin:
            reply_text(reply_token, "Bạn không có quyền admin.")
            return True

        parts = text.split()
        print(f"[COMMAND] /revoke parts={parts}")

        if len(parts) != 2:
            reply_text(reply_token, "Cú pháp: /revoke USER_ID")
            return True

        target_uid = normalize_id(parts[1])
        print(f"[COMMAND] revoke target_uid={repr(target_uid)}")

        success = set_user_premium(target_uid, False)

        if success:
            reply_text(reply_token, f"Đã gỡ premium cho {target_uid}")
        else:
            reply_text(reply_token, "Gỡ premium thất bại")

        return True

    return False

# =========================================================
# MESSAGE FLOW
# =========================================================
@handler.add(MessageEvent, message=TextMessage)
def handle_message(event):
    user_id = normalize_id(event.source.user_id)
    text = (event.message.text or "").strip()

    print(f"[MESSAGE] user_id={repr(user_id)} text={repr(text)}")

    is_command = process_commands(event)
    if is_command:
        print("[FLOW] command handled, stop translate flow")
        return

    print(f"[TRANSLATE FLOW] processing={repr(text)}")
    # Tạm thời để tránh nhiễu debug /grant
    reply_text(event.reply_token, f"[AUTO -> en]\n{text}")

# =========================================================
# MAIN
# =========================================================
if __name__ == "__main__":
    port = int(os.getenv("PORT", "10000"))
    print(f"[BOOT] Running on 0.0.0.0:{port}")
    app.run(host="0.0.0.0", port=port)
