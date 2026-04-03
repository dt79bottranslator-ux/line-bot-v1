import os
import json
import re
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
APP_VERSION = "DT79_V10_SUPER_LOCK"

LINE_ACCESS_TOKEN = (os.getenv("LINE_CHANNEL_ACCESS_TOKEN") or "").strip()
LINE_SECRET = (os.getenv("LINE_CHANNEL_SECRET") or "").strip()
SHEET_ID = (os.getenv("GOOGLE_SHEET_ID") or os.getenv("SPREADSHEET_ID") or "").strip()
GOOGLE_JSON = (os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON") or "").strip()
SHEET_NAME = "USER_LANG_MAP"

# [4. AUTH SYSTEM — ADMIN AXIS LOCK]
# ID CỦA ANH DŨNG - ĐÃ KHÓA CỨNG
ADMIN_LIST = ["U83c6ce008a35ef17edaff25ac003370"] 

def normalize_id(val: Any) -> str:
    """Vá lỗi: Xóa sạch khoảng trắng và ép kiểu về string chuẩn."""
    if not val: return ""
    return str(val).strip()

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
# [VÁ LỖI LOGIC ADMIN & CẤP QUYỀN]
# =========================================================
@handler.add(MessageEvent, message=TextMessageContent)
def handle_text(event):
    # Lấy ID người gửi và làm sạch ngay lập tức
    real_uid = normalize_id(event.source.user_id)
    token = event.reply_token
    raw_incoming = event.message.text or ""
    
    # [VÁ LỖI]: Hợp nhất các dòng nếu người dùng nhấn Enter trên điện thoại
    clean_incoming = " ".join(raw_incoming.split()) 

    # Kiểm tra quyền Admin
    is_admin = real_uid in ADMIN_LIST

    # Xử lý các lệnh bắt đầu bằng "/"
    if clean_incoming.startswith("/"):
        # Lệnh kiểm tra ID cá nhân (Dành cho mọi người)
        if clean_incoming.lower() == "/me":
            reply_msg(token, f"🆔 ID của bạn là:\n{real_uid}")
            return

        # CHẶN ADMIN: Nếu không phải admin thì không cho chạy lệnh /grant
        if not is_admin:
            reply_msg(token, f"❌ Quyền Admin bị từ chối.\nID của bạn: {real_uid}")
            return 

        # Xử lý lệnh /grant (Chỉ Admin mới tới được đây)
        if clean_incoming.lower().startswith("/grant"):
            parts = clean_incoming.split()
            if len(parts) < 2:
                reply_msg(token, "Cú pháp: /grant USER_ID")
                return

            # Lấy target ID (Lọc sạch dấu cách dư thừa)
            target = normalize_id(parts[1])
            ws = get_ws()
            if not ws: 
                reply_msg(token, "❌ Lỗi kết nối Google Sheet")
                return

            try:
                # Tìm kiếm và cập nhật trạng thái Premium
                cells = ws.findall(target) 
                now_ts = datetime.now(timezone.utc).isoformat()
                
                if cells:
                    for cell in cells:
                        ws.update_cell(cell.row, 4, "TRUE") # Cột D: Premium Status
                        ws.update_cell(cell.row, 3, now_ts) # Cột C: Timestamp
                    msg = f"✅ [KHỚP DỮ LIỆU]\nUser: {target}\nTrạng thái: ĐÃ NÂNG CẤP PREMIUM"
                else:
                    # Nếu chưa có thì tạo mới dòng người dùng
                    ws.append_row([target, "en", now_ts, "TRUE", "0", "USER", "user"])
                    msg = f"✅ [TẠO MỚI]\nUser: {target}\nTrạng thái: ĐÃ CẤP QUYỀN PREMIUM"
                
                reply_msg(token, msg)
                return 
            except Exception as e:
                reply_msg(token, f"❌ [THẤT BẠI] Lỗi Sheet: {str(e)}")
                return

        return # Kết thúc các lệnh bắt đầu bằng /

    # [PHẦN DÀNH CHO DỊCH THUẬT - SẼ PHÁT TRIỂN TIẾP]
    print(f"[EVENT] Tin nhắn từ {real_uid}: {clean_incoming}")

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 10000)))
