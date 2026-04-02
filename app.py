import os
import json
from datetime import datetime, timezone
from typing import Any, List, Optional

import gspread
import requests
from flask import Flask, request, abort
from oauth2client.service_account import ServiceAccountCredentials
from linebot.v3.messaging import Configuration, ApiClient, MessagingApi, ReplyMessageRequest, TextMessage as V3TextMessage
from linebot.v3.webhook import WebhookHandler
from linebot.v3.exceptions import InvalidSignatureError
from linebot.v3.webhooks import MessageEvent, TextMessageContent

# =========================================================
# APP INIT & CONFIG
# =========================================================
app = Flask(__name__)
APP_VERSION = "DT79_LINE_BOT_ULTRA_FIX_V2"

LINE_CHANNEL_ACCESS_TOKEN = (os.getenv("LINE_CHANNEL_ACCESS_TOKEN") or "").strip()
LINE_CHANNEL_SECRET = (os.getenv("LINE_CHANNEL_SECRET") or "").strip()
GOOGLE_API_KEY = (os.getenv("GOOGLE_API_KEY") or "").strip()
GOOGLE_SHEET_ID = (os.getenv("GOOGLE_SHEET_ID") or "").strip()
GOOGLE_CREDENTIALS_JSON = (os.getenv("GOOGLE_CREDENTIALS_JSON") or os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON") or "").strip()

DEFAULT_ADMIN_UID = "U83c6ce008a35ef17edaff25ac003370"
ADMIN_LIST = [x.strip() for x in (os.getenv("ADMIN_LIST") or DEFAULT_ADMIN_UID).split(",") if x.strip()]

USER_LANG_SHEET_NAME = "USER_LANG_MAP"
# Columns Index (0-based)
COL_USER_ID, COL_TARGET_LANG, COL_UPDATED_AT, COL_IS_PREMIUM, COL_USAGE_COUNT, COL_GROUP_ID, COL_ROLE = 0, 1, 2, 3, 4, 5, 6

configuration = Configuration(access_token=LINE_CHANNEL_ACCESS_TOKEN)
handler = WebhookHandler(LINE_CHANNEL_SECRET)

# =========================================================
# UTILS
# =========================================================
def now_iso(): return datetime.now(timezone.utc).isoformat()

def normalize_id(val: Any): return str(val or "").strip().replace("\u200b","").replace("\ufeff","")

# =========================================================
# GOOGLE SHEET ENGINE (FIXED & OPTIMIZED)
# =========================================================
def get_worksheet():
    try:
        creds = ServiceAccountCredentials.from_json_keyfile_dict(json.loads(GOOGLE_CREDENTIALS_JSON), 
                ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"])
        client = gspread.authorize(creds)
        return client.open_by_key(GOOGLE_SHEET_ID).worksheet(USER_LANG_SHEET_NAME)
    except Exception as e:
        print(f"[SHEET ERROR] {e}")
        return None

def sync_user_data(user_id: str, updates: dict) -> bool:
    """Hàm hợp nhất: Tự động tìm và cập nhật hoặc thêm mới nếu chưa có"""
    ws = get_worksheet()
    if not ws: return False
    
    uid = normalize_id(user_id)
    try:
        all_vals = ws.get_all_values()
        row_idx = None
        current_data = []

        for i, row in enumerate(all_vals):
            if i > 0 and len(row) > 0 and normalize_id(row[0]) == uid:
                row_idx = i + 1
                current_data = row
                break
        
        # Chuẩn bị dữ liệu dòng (đảm bảo đủ 7 cột)
        if not current_data:
            current_data = [uid, "en", now_iso(), "FALSE", "0", "USER", "user"]
        
        # Ghi đè các giá trị mới từ dictionary updates
        # Ví dụ updates = {COL_IS_PREMIUM: "TRUE"}
        for col_idx, val in updates.items():
            while len(current_data) <= col_idx: current_data.append("")
            current_data[col_idx] = str(val)
        
        current_data[COL_UPDATED_AT] = now_iso()

        if row_idx:
            ws.update(f"A{row_idx}:G{row_idx}", [current_data])
        else:
            ws.append_row(current_data)
        return True
    except Exception as e:
        print(f"[SYNC ERROR] {e}")
        return False

# =========================================================
# LOGIC FUNCTIONS
# =========================================================
def get_user_lang(user_id: str) -> str:
    ws = get_worksheet()
    if not ws: return "en"
    uid = normalize_id(user_id)
    for row in ws.get_all_values():
        if normalize_id(row[0]) == uid:
            return row[COL_TARGET_LANG] if len(row) > 1 else "en"
    return "en"

def reply_msg(token, text):
    with ApiClient(configuration) as api_client:
        api_instance = MessagingApi(api_client)
        api_instance.reply_message(ReplyMessageRequest(
            reply_token=token,
            messages=[V3TextMessage(text=text)]
        ))

# =========================================================
# WEBHOOK & HANDLERS
# =========================================================
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

    # COMMANDS
    if text.startswith("/"):
        if uid not in ADMIN_LIST:
            return reply_msg(token, "Từ chối: Bạn không có quyền Admin.")
            
        parts = text.split()
        cmd = parts[0].lower()

        if cmd == "/grant" and len(parts) == 2:
            target = normalize_id(parts[1])
            if sync_user_data(target, {COL_IS_PREMIUM: "TRUE"}):
                reply_msg(token, f"✅ Đã cấp Premium cho: {target}")
            else:
                reply_msg(token, "❌ Lỗi kết nối Sheet.")
            return

        if cmd == "/revoke" and len(parts) == 2:
            target = normalize_id(parts[1])
            sync_user_data(target, {COL_IS_PREMIUM: "FALSE"})
            reply_msg(token, f"🚫 Đã gỡ Premium: {target}")
            return

    # TRANSLATION FLOW
    lang = get_user_lang(uid)
    # Giả định hàm translate_text đã có sẵn từ code cũ
    # translated = translate_text(text, lang)
    # reply_msg(token, f"[AUTO -> {lang}]\n{translated}")
    reply_msg(token, f"Đang sử dụng ngôn ngữ: {lang}. (Hệ thống dịch đang chờ payload)")

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 10000)))
