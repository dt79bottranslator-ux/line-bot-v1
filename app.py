import os
import json
from datetime import datetime, timezone
from typing import Any

import gspread
from flask import Flask, request, abort
from oauth2client.service_account import ServiceAccountCredentials

from linebot.v3.messaging import Configuration, ApiClient, MessagingApi, ReplyMessageRequest, TextMessage as V3TextMessage
from linebot.v3.webhook import WebhookHandler
from linebot.v3.exceptions import InvalidSignatureError
from linebot.v3.webhooks import MessageEvent, TextMessageContent

app = Flask(__name__)
APP_VERSION = "DT79_ULTRA_CLEAN_V4_DEBUG"

# Config
LINE_CHANNEL_ACCESS_TOKEN = (os.getenv("LINE_CHANNEL_ACCESS_TOKEN") or "").strip()
LINE_CHANNEL_SECRET = (os.getenv("LINE_CHANNEL_SECRET") or "").strip()
GOOGLE_SHEET_ID = (os.getenv("GOOGLE_SHEET_ID") or os.getenv("SPREADSHEET_ID") or "").strip()
GOOGLE_CREDENTIALS_JSON = (os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON") or "").strip()

# Admin Logic
DEFAULT_ADMIN = "U83c6ce008a35ef17edaff25ac003370"
ADMIN_LIST = [x.strip() for x in (os.getenv("ADMIN_LIST") or DEFAULT_ADMIN).split(",") if x.strip()]

SHEET_NAME = "USER_LANG_MAP"
COL_IS_PREMIUM, COL_UPDATED_AT = 3, 2

configuration = Configuration(access_token=LINE_CHANNEL_ACCESS_TOKEN)
handler = WebhookHandler(LINE_CHANNEL_SECRET)

print(f"[BOOT] {APP_VERSION} starting...")
print(f"[BOOT] Current Admins in RAM: {ADMIN_LIST}")

def now_iso(): return datetime.now(timezone.utc).isoformat()
def normalize_id(val: Any): return str(val or "").strip().replace("\u200b","").replace("\ufeff","")

def reply_msg(token, text):
    with ApiClient(configuration) as api_client:
        api_instance = MessagingApi(api_client)
        api_instance.reply_message(ReplyMessageRequest(
            reply_token=token,
            messages=[V3TextMessage(text=text)]
        ))

def get_ws():
    try:
        creds_dict = json.loads(GOOGLE_CREDENTIALS_JSON)
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
        client = gspread.authorize(creds)
        return client.open_by_key(GOOGLE_SHEET_ID).worksheet(SHEET_NAME)
    except Exception as e:
        print(f"[SHEET ERROR] {e}")
        return None

def sync_user_premium(target_id: str, is_premium: bool) -> tuple[bool, str]:
    ws = get_ws()
    if not ws: return False, "API Connection Fail"
    uid = normalize_id(target_id)
    try:
        all_rows = ws.get_all_values()
        row_idx = next((i + 1 for i, r in enumerate(all_rows) if i > 0 and len(r) > 0 and normalize_id(r[0]) == uid), None)
        p_val = "TRUE" if is_premium else "FALSE"
        if row_idx:
            ws.update_cell(row_idx, COL_IS_PREMIUM + 1, p_val)
            ws.update_cell(row_idx, COL_UPDATED_AT + 1, now_iso())
            return True, ""
        else:
            ws.append_row([uid, "en", now_iso(), p_val, "0", "USER", "user"])
            return True, ""
    except Exception as e: return False, str(e)

@app.route("/", methods=["GET"])
def home(): return f"{APP_VERSION}: ONLINE", 200

@app.route("/webhook", methods=["POST"])
def callback():
    sig = request.headers.get("X-Line-Signature", "")
    body = request.get_data(as_text=True)
    try: handler.handle(body, sig)
    except InvalidSignatureError: abort(400)
    return "OK"

@handler.add(MessageEvent, message=TextMessageContent)
def handle_text(event):
    raw_uid = event.source.user_id
    uid = normalize_id(raw_uid)
    text = event.message.text.strip()
    token = event.reply_token

    # VŨ KHÍ HẬU KIỂM: In UID thực tế ra log để đối soát ENV
    print(f"[TARGET_UID] {uid}")

    if text.startswith("/"):
        if uid not in ADMIN_LIST:
            print(f"[AUTH DENIED] UID {uid} not in {ADMIN_LIST}")
            return reply_msg(token, f"❌ Từ chối Admin.\nUID của bạn: {uid}")
            
        parts = text.split()
        if parts[0].lower() == "/grant" and len(parts) == 2:
            target = normalize_id(parts[1])
            success, err = sync_user_premium(target, True)
            reply_msg(token, f"✅ Đã cấp Premium cho {target}" if success else f"❌ Lỗi Sheet: {err}")

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 10000)))
