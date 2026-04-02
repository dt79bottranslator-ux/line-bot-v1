import os
import json
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
# APP INIT
# =========================================================
app = Flask(__name__)
APP_VERSION = "DT79_V6_FINAL_STABLE_AUTO_JOIN"

# =========================================================
# ENV CONFIG
# =========================================================
LINE_ACCESS_TOKEN = (os.getenv("LINE_CHANNEL_ACCESS_TOKEN") or "").strip()
LINE_SECRET = (os.getenv("LINE_CHANNEL_SECRET") or "").strip()
SHEET_ID = (os.getenv("GOOGLE_SHEET_ID") or os.getenv("SPREADSHEET_ID") or "").strip()
GOOGLE_JSON = (os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON") or "").strip()
SHEET_NAME = "USER_LANG_MAP"

# =========================================================
# NORMALIZE (HÀM LÀM SẠCH UID)
# =========================================================
def normalize_id(val: Any) -> str:
    return (
        str(val or "")
        .strip()
        .replace("\u200b", "")
        .replace("\ufeff", "")
        .replace("\u2060", "")
        .replace("\xa0", "")
        .replace("\n", "")
        .replace("\r", "")
        .replace(" ", "")
    )

# =========================================================
# ADMIN CONFIG
# =========================================================
def get_admins() -> List[str]:
    # Hard-code UID của anh để đảm bảo không bao giờ mất quyền Admin
    default_admin = "U83c6ce008a35ef17edaff25ac003370"
    raw = os.getenv("ADMIN_LIST") or default_admin
    return [normalize_id(x) for x in raw.split(",") if normalize_id(x)]

ADMIN_LIST = get_admins()

# =========================================================
# LINE INIT
# =========================================================
configuration = Configuration(access_token=LINE_ACCESS_TOKEN)
handler = WebhookHandler(LINE_SECRET)

print(f"[BOOT] {APP_VERSION}")
print(f"[BOOT] ADMIN_LIST CLEAN: {ADMIN_LIST}")

# =========================================================
# REPLY
# =========================================================
def reply_msg(token, text):
    with ApiClient(configuration) as api_client:
        MessagingApi(api_client).reply_message(
            ReplyMessageRequest(
                reply_token=token,
                messages=[V3TextMessage(text=text)]
            )
        )

# =========================================================
# GOOGLE SHEET
# =========================================================
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

# =========================================================
# ROUTES
# =========================================================
@app.route("/", methods=["GET"])
def home():
    return f"{APP_VERSION} LIVE", 200

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
# MAIN HANDLER
# =========================================================
@handler.add(MessageEvent, message=TextMessageContent)
def handle_text(event):
    real_uid = normalize_id(event.source.user_id)
    token = event.reply_token
    
    # 🔥 CẢI TIẾN QUAN TRỌNG: Tự động gom dòng bị ngắt trên điện thoại
    # Biến "/grant\nUID" thành "/grant UID"
    raw_incoming = event.message.text or ""
    clean_incoming = " ".join(raw_incoming.split()) 
    
    print(f"[INCOMING RAW] {repr(raw_incoming)}")
    print(f"[INCOMING CLEAN] {repr(clean_incoming)}")

    # =====================================================
    # COMMAND LAYER
    # =====================================================
    if clean_incoming.startswith("/"):
        if real_uid not in ADMIN_LIST:
            return reply_msg(token, f"❌ Bạn không có quyền Admin.\nID: {real_uid}")

        parts = clean_incoming.split()
        cmd = parts[0].lower()

        # Lệnh /GRANT
        if cmd == "/grant":
            if len(parts) < 2:
                return reply_msg(token, "Cú pháp: /grant USER_ID")

            # Ghép tất cả phần còn lại (phòng hờ UID bị dính khoảng trắng)
            target = normalize_id("".join(parts[1:]))
            
            ws = get_ws()
            if not ws: return reply_msg(token, "❌ Lỗi kết nối Sheet")

            try:
                rows = ws.get_all_values()
                row_idx = None
                for i, r in enumerate(rows):
                    if i == 0: continue
                    if normalize_id(r[0]) == target:
                        row_idx = i + 1
                        break

                now_ts = datetime.now(timezone.utc).isoformat()
                if row_idx:
                    ws.update_cell(row_idx, 4, "TRUE")
                    ws.update_cell(row_idx, 3, now_ts)
                else:
                    ws.append_row([target, "en", now_ts, "TRUE", "0", "USER", "user"])

                return reply_msg(token, f"✅ Đã cấp Premium thành công cho:\n{target}")

            except Exception as e:
                return reply_msg(token, f"❌ Lỗi ghi Sheet: {str(e)}")

    # Trả về thông tin UID để Admin dễ copy
    return reply_msg(token, f"🆔 UID của bạn:\n{real_uid}")

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 10000)))
