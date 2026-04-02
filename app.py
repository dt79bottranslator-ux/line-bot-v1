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
APP_VERSION = "DT79_V5_FINAL_STABLE"

# Config nạp từ ENV
LINE_ACCESS_TOKEN = (os.getenv("LINE_CHANNEL_ACCESS_TOKEN") or "").strip()
LINE_SECRET = (os.getenv("LINE_CHANNEL_SECRET") or "").strip()
SHEET_ID = (os.getenv("GOOGLE_SHEET_ID") or os.getenv("SPREADSHEET_ID") or "").strip()
GOOGLE_JSON = (os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON") or "").strip()

# Admin Logic - TỰ ĐỘNG LÀM SẠCH KÝ TỰ LẠ
def get_admins():
    raw_admins = os.getenv("ADMIN_LIST") or "U83c6ce008a35ef17edaff25ac003370"
    # Tách dấu phẩy, xóa khoảng trắng và ký tự điều khiển ẩn
    return [x.strip().replace("\u200b","").replace("\ufeff","") for x in raw_admins.split(",") if x.strip()]

ADMIN_LIST = get_admins()
SHEET_NAME = "USER_LANG_MAP"

configuration = Configuration(access_token=LINE_ACCESS_TOKEN)
handler = WebhookHandler(LINE_SECRET)

print(f"[BOOT] {APP_VERSION} starting...")
print(f"[BOOT] Validated Admins: {ADMIN_LIST}")

def normalize_id(val: Any): 
    return str(val or "").strip().replace("\u200b","").replace("\ufeff","")

def reply_msg(token, text):
    with ApiClient(configuration) as api_client:
        api_instance = MessagingApi(api_client)
        api_instance.reply_message(ReplyMessageRequest(
            reply_token=token,
            messages=[V3TextMessage(text=text)]
        ))

def get_ws():
    try:
        creds = ServiceAccountCredentials.from_json_keyfile_dict(json.loads(GOOGLE_JSON), 
                ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"])
        return gspread.authorize(creds).open_by_key(SHEET_ID).worksheet(SHEET_NAME)
    except Exception as e:
        print(f"[SHEET ERROR] {e}")
        return None

@app.route("/", methods=["GET"])
def home(): return f"{APP_VERSION} is LIVE", 200

@app.route("/webhook", methods=["POST"])
def callback():
    sig = request.headers.get("X-Line-Signature", "")
    body = request.get_data(as_text=True)
    try: handler.handle(body, sig)
    except InvalidSignatureError: abort(400)
    return "OK"

@handler.add(MessageEvent, message=TextMessageContent)
def handle_text(event):
    uid = normalize_id(event.source.user_id)
    text = event.message.text.strip()
    token = event.reply_token

    print(f"[INCOMING] UID: {uid} | Text: {text}")

    if text.startswith("/"):
        if uid not in ADMIN_LIST:
            print(f"[AUTH DENIED] {uid} is not in {ADMIN_LIST}")
            return reply_msg(token, f"❌ Bạn không có quyền Admin.\nID của bạn: {uid}")
            
        parts = text.split()
        if parts[0].lower() == "/grant" and len(parts) == 2:
            target = normalize_id(parts[1])
            ws = get_ws()
            if not ws: return reply_msg(token, "❌ Lỗi kết nối Google Sheet.")
            
            try:
                all_rows = ws.get_all_values()
                row_idx = next((i + 1 for i, r in enumerate(all_rows) if i > 0 and normalize_id(r[0]) == target), None)
                
                if row_idx:
                    ws.update_cell(row_idx, 4, "TRUE") # Cột Premium
                    ws.update_cell(row_idx, 3, datetime.now(timezone.utc).isoformat())
                else:
                    ws.append_row([target, "en", datetime.now(timezone.utc).isoformat(), "TRUE", "0", "USER", "user"])
                
                reply_msg(token, f"✅ Đã cấp Premium cho {target}")
            except Exception as e:
                reply_msg(token, f"❌ Lỗi: {str(e)}")

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 10000)))
