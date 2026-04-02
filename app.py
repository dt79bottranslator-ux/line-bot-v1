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
APP_VERSION = "DT79_V5_FINAL_ADMIN_FIX"

# =========================================================
# ENV CONFIG
# =========================================================
LINE_ACCESS_TOKEN = (os.getenv("LINE_CHANNEL_ACCESS_TOKEN") or "").strip()
LINE_SECRET = (os.getenv("LINE_CHANNEL_SECRET") or "").strip()
SHEET_ID = (os.getenv("GOOGLE_SHEET_ID") or os.getenv("SPREADSHEET_ID") or "").strip()
GOOGLE_JSON = (os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON") or "").strip()

SHEET_NAME = "USER_LANG_MAP"

# =========================================================
# NORMALIZE (FIX CHÍ MẠNG)
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
    )

# =========================================================
# ADMIN CONFIG
# =========================================================
def get_admins() -> List[str]:
    raw = os.getenv("ADMIN_LIST") or "U83c6ce008a35ef17edaff25ac003370"
    return [normalize_id(x) for x in raw.split(",") if normalize_id(x)]

ADMIN_LIST = get_admins()

# =========================================================
# LINE INIT
# =========================================================
configuration = Configuration(access_token=LINE_ACCESS_TOKEN)
handler = WebhookHandler(LINE_SECRET)

# =========================================================
# BOOT LOG
# =========================================================
print(f"[BOOT] {APP_VERSION}")
print(f"[BOOT] ADMIN_LIST RAW: {os.getenv('ADMIN_LIST')}")
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
            [
                "https://spreadsheets.google.com/feeds",
                "https://www.googleapis.com/auth/drive",
            ],
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
    text = (event.message.text or "").strip()
    token = event.reply_token

    print(f"[REAL USER ID] {repr(real_uid)}")
    print(f"[INCOMING] text={repr(text)}")

    # =====================================================
    # COMMAND LAYER (CHẶN TUYỆT ĐỐI)
    # =====================================================
    if text.startswith("/"):

        # 🔥 FIX CHÍ MẠNG Ở ĐÂY
        admin_clean = [normalize_id(x) for x in ADMIN_LIST]

        print(f"[ADMIN CHECK] real={repr(real_uid)} vs list={admin_clean}")

        if real_uid not in admin_clean:
            print("[AUTH DENIED]")
            return reply_msg(token, f"❌ Bạn không có quyền Admin.\nID: {real_uid}")

        print("[ADMIN PASS]")

        parts = text.split()

        # =================================================
        # /GRANT
        # =================================================
        if parts[0].lower() == "/grant":
            if len(parts) != 2:
                return reply_msg(token, "Cú pháp: /grant USER_ID")

            target = normalize_id(parts[1])
            print(f"[TARGET] {repr(target)}")

            ws = get_ws()
            if not ws:
                return reply_msg(token, "❌ Lỗi kết nối Sheet")

            try:
                rows = ws.get_all_values()

                row_idx = None
                for i, r in enumerate(rows):
                    if i == 0:
                        continue
                    if normalize_id(r[0]) == target:
                        row_idx = i + 1
                        break

                if row_idx:
                    ws.update_cell(row_idx, 4, "TRUE")
                    ws.update_cell(row_idx, 3, datetime.now(timezone.utc).isoformat())
                    print(f"[MATCH FOUND] row={row_idx}")
                else:
                    ws.append_row([
                        target,
                        "en",
                        datetime.now(timezone.utc).isoformat(),
                        "TRUE",
                        "0",
                        "USER",
                        "user",
                    ])
                    print("[APPEND NEW USER]")

                return reply_msg(token, f"✅ Premium: {target}")

            except Exception as e:
                print(f"[SHEET WRITE ERROR] {e}")
                return reply_msg(token, "❌ Lỗi ghi Sheet")

        return reply_msg(token, "❌ Lệnh không hợp lệ")

    # =====================================================
    # NON-COMMAND
    # =====================================================
    return reply_msg(token, f"[DEBUG] UID: {real_uid}")

# =========================================================
# MAIN
# =========================================================
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 10000)))
