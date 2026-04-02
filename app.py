import os
import json
import logging
from datetime import datetime, timezone
from typing import Any, List

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
# [1. SYSTEM IDENTITY & CONFIG]
# =========================================================
app = Flask(__name__)
APP_VERSION = "DT79_V8_ULTRA_STABLE_REPO_RENDER_1"

LINE_ACCESS_TOKEN = (os.getenv("LINE_CHANNEL_ACCESS_TOKEN") or "").strip()
LINE_SECRET = (os.getenv("LINE_CHANNEL_SECRET") or "").strip()
SHEET_ID = (os.getenv("GOOGLE_SHEET_ID") or os.getenv("SPREADSHEET_ID") or "").strip()
GOOGLE_JSON = (os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON") or "").strip()
SHEET_NAME = "USER_LANG_MAP"

# [4. AUTH SYSTEM — ADMIN AXIS LOCK]
ADMIN_LIST = ["U83c6ce008a35ef17edaff25ac003370"] 

def normalize_id(val: Any) -> str:
    return str(val or "").strip().replace(" ", "").replace("\n", "").replace("\r", "")

# =========================================================
# [LINE & SHEET INIT]
# =========================================================
configuration = Configuration(access_token=LINE_ACCESS_TOKEN)
handler = WebhookHandler(LINE_SECRET)

def get_ws():
    try:
        creds = ServiceAccountCredentials.from_json_keyfile_dict(
            json.loads(GOOGLE_JSON),
            ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"],
        )
        return gspread.authorize(creds).open_by_key(SHEET_ID).worksheet(SHEET_NAME)
    except Exception as e:
        print(f"[SHEET ERROR] {e}")
        return None

def reply_msg(token, text):
    with ApiClient(configuration) as api_client:
        MessagingApi(api_client).reply_message(
            ReplyMessageRequest(
                reply_token=token,
                messages=[V3TextMessage(text=text)]
            )
        )

# =========================================================
# [ROUTES]
# =========================================================
@app.route("/", methods=["GET"])
def home():
    return f"{APP_VERSION} - REPO: line-bot-render-1 LIVE", 200

@app.route("/webhook", methods=["POST"])
def callback():
    sig = request.headers.get("X-Line-Signature", "")
    body = request.get_data(as_text=True)
    try:
        handler.handle(body, sig)
    except InvalidSignatureError:
        abort(400)
    return "OK"

# =========================================================
# [7. DEBUG PROTOCOL & 5. COMMAND LOCK]
# =========================================================
@handler.add(MessageEvent, message=TextMessageContent)
def handle_text(event):
    # [AUTH CHECK] 
    real_uid = normalize_id(event.source.user_id)
    token = event.reply_token
    raw_incoming = event.message.text or ""
    
    # [9. KNOWN BUGS FIX] Gom dòng cho điện thoại
    clean_incoming = " ".join(raw_incoming.split()) 

    # [5. FLOW CONTROL — COMMAND LOCK]
    if clean_incoming.startswith("/"):
        print(f"[AUTH CHECK] uid='{real_uid}'")
        print(f"[AUTH CHECK] admin_list={ADMIN_LIST}")
        
        # [4. AUTH SYSTEM LOCK]
        is_admin = real_uid in ADMIN_LIST
        print(f"[AUTH CHECK] match={is_admin}")

        if not is_admin:
            reply_msg(token, f"❌ Quyền Admin bị từ chối.\nID của bạn: {real_uid}")
            return # Thoát ngay

        # Xử lý lệnh /grant
        if clean_incoming.lower().startswith("/grant"):
            parts = clean_incoming.split()
            if len(parts) < 2:
                reply_msg(token, "Cú pháp: /grant USER_ID")
                return

            target = normalize_id(parts[1])
            ws = get_ws()
            if not ws: 
                reply_msg(token, "❌ Lỗi kết nối Sheet")
                return

            try:
                # [DATA FLOW] Tìm và cập nhật
                cells = ws.findall(target) # Tìm nhanh theo UID
                now_ts = datetime.now(timezone.utc).isoformat()
                
                if cells:
                    for cell in cells:
                        ws.update_cell(cell.row, 4, "TRUE") # Cột D: Premium
                        ws.update_cell(cell.row, 3, now_ts) # Cột C: Time
                    msg = f"✅ [MATCH FOUND]\nUser: {target}\nStatus: PREMIUM SET"
                else:
                    # Nếu chưa có thì thêm mới
                    ws.append_row([target, "en", now_ts, "TRUE", "0", "USER", "user"])
                    msg = f"✅ [NEW RECORD]\nUser: {target}\nStatus: PREMIUM CREATED"
                
                reply_msg(token, msg)
                return # NGẮT LUỒNG SAU KHI XỬ LÝ XONG
            except Exception as e:
                reply_msg(token, f"❌ [MATCH FAILED] Lỗi: {str(e)}")
                return

        return # Ngắt mọi lệnh bắt đầu bằng / khác

    # =====================================================
    # [TRANSLATION FLOW] - CHỈ CHẠY NẾU KHÔNG PHẢI COMMAND
    # =====================================================
    # Logic dịch của anh để ở đây
    print(f"[EVENT] Processing text: {clean_incoming}")
    # reply_msg(token, f"ID của bạn: {real_uid}")

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 10000)))
