import os
import json
from datetime import datetime, timezone
from typing import Any, List, Optional

import gspread
import requests
from flask import Flask, request, abort
from oauth2client.service_account import ServiceAccountCredentials

# Import Line SDK v3
from linebot.v3.messaging import Configuration, ApiClient, MessagingApi, ReplyMessageRequest, TextMessage as V3TextMessage
from linebot.v3.webhook import WebhookHandler
from linebot.v3.exceptions import InvalidSignatureError
from linebot.v3.webhooks import MessageEvent, TextMessageContent

# =========================================================
# CONFIGURATION & INIT
# =========================================================
app = Flask(__name__)
APP_VERSION = "DT79_ULTRA_CLEAN_V3_FINAL"

# Env vars
LINE_CHANNEL_ACCESS_TOKEN = (os.getenv("LINE_CHANNEL_ACCESS_TOKEN") or "").strip()
LINE_CHANNEL_SECRET = (os.getenv("LINE_CHANNEL_SECRET") or "").strip()
GOOGLE_API_KEY = (os.getenv("GOOGLE_API_KEY") or "").strip()
GOOGLE_SHEET_ID = (os.getenv("GOOGLE_SHEET_ID") or "").strip()
GOOGLE_CREDENTIALS_JSON = (os.getenv("GOOGLE_CREDENTIALS_JSON") or os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON") or "").strip()

# Admin check
DEFAULT_ADMIN = "U83c6ce008a35ef17edaff25ac003370"
ADMIN_LIST = [x.strip() for x in (os.getenv("ADMIN_LIST") or DEFAULT_ADMIN).split(",") if x.strip()]

# Sheet structure
USER_LANG_SHEET_NAME = "USER_LANG_MAP"
COL_USER_ID, COL_TARGET_LANG, COL_UPDATED_AT, COL_IS_PREMIUM, COL_USAGE_COUNT, COL_GROUP_ID, COL_ROLE = 0, 1, 2, 3, 4, 5, 6

configuration = Configuration(access_token=LINE_CHANNEL_ACCESS_TOKEN)
handler = WebhookHandler(LINE_CHANNEL_SECRET)

print(f"[BOOT] {APP_VERSION} starting...")

# =========================================================
# CORE UTILS
# =========================================================
def now_iso(): return datetime.now(timezone.utc).isoformat()

def normalize_id(val: Any): 
    return str(val or "").strip().replace("\u200b","").replace("\ufeff","").replace("\n","").replace("\r","")

def reply_msg(token, text):
    with ApiClient(configuration) as api_client:
        api_instance = MessagingApi(api_client)
        api_instance.reply_message(ReplyMessageRequest(
            reply_token=token,
            messages=[V3TextMessage(text=text)]
        ))

# =========================================================
# GOOGLE SHEET LOGIC
# =========================================================
def get_ws():
    try:
        creds_dict = json.loads(GOOGLE_CREDENTIALS_JSON)
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
        client = gspread.authorize(creds)
        return client.open_by_key(GOOGLE_SHEET_ID).worksheet(USER_LANG_SHEET_NAME)
    except Exception as e:
        print(f"[SHEET ERROR] Connection failed: {e}")
        return None

def sync_user_premium(target_id: str, is_premium: bool) -> tuple[bool, str]:
    """Trả về (Thành công, Nội dung lỗi)"""
    ws = get_ws()
    if not ws: return False, "Không thể kết nối Google Sheet"
    
    uid = normalize_id(target_id)
    try:
        all_rows = ws.get_all_values()
        row_idx = None
        
        # Tìm kiếm User
        for i, row in enumerate(all_rows):
            if i > 0 and len(row) > 0 and normalize_id(row[0]) == uid:
                row_idx = i + 1
                break
        
        premium_str = "TRUE" if is_premium else "FALSE"
        
        if row_idx:
            # Chỉ cập nhật 2 cột cần thiết để tối ưu quota
            ws.update_cell(row_idx, COL_IS_PREMIUM + 1, premium_str)
            ws.update_cell(row_idx, COL_UPDATED_AT + 1, now_iso())
            print(f"[MATCH FOUND] Updated row {row_idx} for {uid}")
            return True, ""
        else:
            # Tạo mới nếu không thấy
            new_row = [uid, "en", now_iso(), premium_str, "0", "USER", "user"]
            ws.append_row(new_row)
            print(f"[MATCH FAILED] Created new row for {uid}")
            return True, ""
            
    except Exception as e:
        print(f"[WRITE ERROR] {e}")
        return False, str(e)

# =========================================================
# ROUTES & HANDLERS
# =========================================================
@app.route("/", methods=["GET"])
def home():
    return f"{APP_VERSION} - STATUS: ONLINE", 200

@app.route("/webhook", methods=["POST"])
def callback():
    signature = request.headers.get("X-Line-Signature", "")
    body = request.get_data(as_text=True)
    try:
        handler.handle(body, signature)
    except InvalidSignatureError: abort(400)
    return "OK"

@handler.add(MessageEvent, message=TextMessageContent)
def handle_text(event):
    uid = normalize_id(event.source.user_id)
    text = event.message.text.strip()
    token = event.reply_token

    # COMMAND HANDLER
    if text.startswith("/"):
        if uid not in ADMIN_LIST:
            print(f"[AUTH DENIED] UID {uid} tried {text}")
            return reply_msg(token, "❌ Bạn không có quyền Admin.")
            
        parts = text.split()
        cmd = parts[0].lower()

        if cmd == "/grant" and len(parts) == 2:
            target = normalize_id(parts[1])
            success, err = sync_user_premium(target, True)
            if success:
                reply_msg(token, f"✅ Thành công: Đã cấp Premium cho {target}")
            else:
                reply_msg(token, f"❌ Lỗi tầng Sheet: {err}")
            return

    # Mặc định: Echo hoặc dịch (có thể thêm logic dịch ở đây)
    # reply_msg(token, f"Bạn nói: {text}")

if __name__ == "__main__":
    port = int(os.getenv("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
