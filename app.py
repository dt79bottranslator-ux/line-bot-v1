import os
import json
from datetime import datetime, timezone
from typing import Any, List, Optional

import gspread
import requests
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
# ENV
# =========================================================
APP_VERSION = "DT79_LINE_BOT_PRACTICAL_CLEAN_V1"

LINE_CHANNEL_ACCESS_TOKEN = (os.getenv("LINE_CHANNEL_ACCESS_TOKEN") or "").strip()
LINE_CHANNEL_SECRET = (os.getenv("LINE_CHANNEL_SECRET") or "").strip()
GOOGLE_API_KEY = (os.getenv("GOOGLE_API_KEY") or "").strip()
GOOGLE_SHEET_ID = (os.getenv("GOOGLE_SHEET_ID") or "").strip()
GOOGLE_CREDENTIALS_JSON = (
    os.getenv("GOOGLE_CREDENTIALS_JSON")
    or os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON")
    or ""
).strip()

# Admin list:
# Có thể set nhiều UID, ngăn cách bằng dấu phẩy
# Ví dụ:
# ADMIN_LIST=Uxxx,Uyyy
DEFAULT_ADMIN_UID = "U83c6ce008a35ef17edaff25ac003370"
ADMIN_LIST_RAW = (os.getenv("ADMIN_LIST") or DEFAULT_ADMIN_UID).strip()

# =========================================================
# CONSTANTS
# =========================================================
LINE_REPLY_URL = "https://api.line.me/v2/bot/message/reply"
GOOGLE_TRANSLATE_URL = "https://translation.googleapis.com/language/translate/v2"

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
print(f"[BOOT] APP_VERSION={APP_VERSION}")
print("[BOOT] Starting LINE bot")
print(f"[BOOT] LINE_CHANNEL_ACCESS_TOKEN exists: {bool(LINE_CHANNEL_ACCESS_TOKEN)}")
print(f"[BOOT] LINE_CHANNEL_SECRET exists: {bool(LINE_CHANNEL_SECRET)}")
print(f"[BOOT] GOOGLE_API_KEY exists: {bool(GOOGLE_API_KEY)}")
print(f"[BOOT] GOOGLE_SHEET_ID exists: {bool(GOOGLE_SHEET_ID)}")
print(f"[BOOT] GOOGLE_CREDENTIALS_JSON exists: {bool(GOOGLE_CREDENTIALS_JSON)}")
print(f"[BOOT] ADMIN_LIST_RAW={ADMIN_LIST_RAW}")

# =========================================================
# CORE UTILS
# =========================================================
def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def normalize_id(value: Any) -> str:
    if value is None:
        return ""

    text = str(value)
    text = text.replace("\u200b", "")   # zero-width space
    text = text.replace("\ufeff", "")   # BOM
    text = text.replace("\u2060", "")   # word joiner
    text = text.replace("\xa0", " ")    # non-breaking space
    text = text.replace("\n", "")
    text = text.replace("\r", "")
    return text.strip()


def safe_str(value: Any) -> str:
    return str(value or "").strip()


def get_admin_list() -> List[str]:
    return [normalize_id(x) for x in ADMIN_LIST_RAW.split(",") if normalize_id(x)]


def normalize_target_lang(raw_lang: str) -> Optional[str]:
    mapping = {
        "zh": "zh-TW",
        "zh-tw": "zh-TW",
        "tw": "zh-TW",
        "en": "en",
        "vi": "vi",
        "ja": "ja",
        "jp": "ja",
        "ko": "ko",
        "th": "th",
        "id": "id",
    }
    return mapping.get(safe_str(raw_lang).lower())


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


def get_worksheet(sheet_name: str):
    if not GOOGLE_SHEET_ID:
        print("[SHEET ERROR] GOOGLE_SHEET_ID missing")
        return None

    client = get_gspread_client()
    if client is None:
        return None

    try:
        spreadsheet = client.open_by_key(GOOGLE_SHEET_ID)
        worksheet = spreadsheet.worksheet(sheet_name)
        return worksheet
    except Exception as exc:
        print(f"[SHEET ERROR] open worksheet failed: {str(exc)}")
        return None


def get_row_value(row: List[str], col_index: int, default: str = "") -> str:
    if len(row) > col_index:
        return safe_str(row[col_index])
    return default


def find_user_row_index(worksheet, user_id: str) -> Optional[int]:
    target_user_id = normalize_id(user_id)

    if worksheet is None:
        print("[FIND USER] worksheet=None")
        return None

    try:
        values = worksheet.get_all_values()
        print(f"[FIND USER] target={repr(target_user_id)} total_rows={len(values)}")

        for idx, row in enumerate(values):
            if idx == 0:
                continue

            raw_sheet_id = row[COL_USER_ID] if len(row) > COL_USER_ID else ""
            row_user_id = normalize_id(raw_sheet_id)

            print(
                f"[COMPARE] row={idx + 1} "
                f"sheet_raw={repr(raw_sheet_id)} "
                f"sheet_norm={repr(row_user_id)} "
                f"target={repr(target_user_id)}"
            )

            if row_user_id == target_user_id:
                print(f"[MATCH FOUND] row_index={idx + 1}")
                return idx + 1

        print("[MATCH FAILED]")
        return None

    except Exception as exc:
        print(f"[FIND USER ERROR] {str(exc)}")
        return None


def get_user_row(user_id: str) -> Optional[List[str]]:
    ws = get_worksheet(USER_LANG_SHEET_NAME)
    if ws is None:
        return None

    row_index = find_user_row_index(ws, user_id)
    if not row_index:
        return None

    try:
        return ws.row_values(row_index)
    except Exception as exc:
        print(f"[SHEET ERROR] row_values failed: {str(exc)}")
        return None


def get_user_role(user_id: str) -> str:
    row = get_user_row(user_id)
    if row is None:
        print("[ROLE DEBUG] row=None")
        return ""

    raw_role = get_row_value(row, COL_ROLE, "")
    role = normalize_id(raw_role).lower()
    print(f"[ROLE DEBUG] raw={repr(raw_role)} normalized={repr(role)}")
    return role


def is_user_admin(user_id: str) -> bool:
    user_id_norm = normalize_id(user_id)

    # Ưu tiên env admin list
    admin_list = get_admin_list()
    if user_id_norm in admin_list:
        print(f"[ADMIN] user_id={repr(user_id_norm)} admin=True source=env")
        return True

    # Fallback: đọc role trong sheet
    role = get_user_role(user_id_norm)
    result = role == "admin"
    print(f"[ADMIN] user_id={repr(user_id_norm)} role={repr(role)} admin={result} source=sheet")
    return result


def set_user_premium(user_id: str, premium: bool) -> bool:
    ws = get_worksheet(USER_LANG_SHEET_NAME)
    if ws is None:
        print("[PREMIUM SET] worksheet unavailable")
        return False

    try:
        target_user_id = normalize_id(user_id)
        row_index = find_user_row_index(ws, target_user_id)

        if not row_index:
            print(f"[PREMIUM SET] user_id not found: {target_user_id}")
            return False

        current_row = ws.row_values(row_index)

        target_lang = get_row_value(current_row, COL_TARGET_LANG, "en") or "en"
        usage_count = get_row_value(current_row, COL_USAGE_COUNT, "0") or "0"
        group_id = get_row_value(current_row, COL_GROUP_ID, "USER") or "USER"
        role = get_row_value(current_row, COL_ROLE, "")

        premium_text = "TRUE" if premium else "FALSE"

        new_row = [
            target_user_id,
            target_lang,
            now_iso(),
            premium_text,
            usage_count,
            group_id,
            role,
        ]

        print(f"[PREMIUM SET] write_range=A{row_index}:G{row_index}")
        print(f"[PREMIUM SET] new_row={new_row}")

        try:
            ws.update(f"A{row_index}:G{row_index}", [new_row])
        except Exception as write_exc:
            print(f"[SHEET WRITE ERROR] {str(write_exc)}")
            return False

        print(f"[PREMIUM SET] row={row_index} user_id={target_user_id} premium={premium_text}")
        return True

    except Exception as exc:
        print(f"[PREMIUM SET ERROR] {str(exc)}")
        return False


def save_user_target_lang(user_id: str, target_lang: str, group_id: str = "USER") -> bool:
    ws = get_worksheet(USER_LANG_SHEET_NAME)
    if ws is None:
        print("[LANG SAVE] worksheet unavailable")
        return False

    try:
        target_uid = normalize_id(user_id)
        row_index = find_user_row_index(ws, target_uid)

        if not row_index:
            print(f"[LANG SAVE] user_id not found: {target_uid}")
            return False

        current_row = ws.row_values(row_index)
        premium_text = get_row_value(current_row, COL_IS_PREMIUM, "FALSE") or "FALSE"
        usage_count = get_row_value(current_row, COL_USAGE_COUNT, "0") or "0"
        role = get_row_value(current_row, COL_ROLE, "")

        new_row = [
            target_uid,
            target_lang,
            now_iso(),
            premium_text,
            usage_count,
            group_id or "USER",
            role,
        ]

        ws.update(f"A{row_index}:G{row_index}", [new_row])
        print(f"[LANG SAVE] row={row_index} user_id={target_uid} target_lang={target_lang}")
        return True

    except Exception as exc:
        print(f"[LANG SAVE ERROR] {str(exc)}")
        return False


def get_user_target_lang(user_id: str, default_lang: str = "en") -> str:
    row = get_user_row(user_id)
    if row is None:
        print(f"[LANG] user_id not found, default={default_lang}")
        return default_lang

    target_lang = get_row_value(row, COL_TARGET_LANG, default_lang) or default_lang
    print(f"[LANG] user_id={normalize_id(user_id)} target_lang={target_lang}")
    return target_lang


# =========================================================
# LINE REPLY
# =========================================================
def reply_text(reply_token: str, text: str):
    try:
        line_bot_api.reply_message(reply_token, TextSendMessage(text=text))
        print(f"[LINE REPLY] text={repr(text)}")
    except Exception as exc:
        print(f"[LINE REPLY ERROR] {str(exc)}")


# =========================================================
# TRANSLATE
# =========================================================
def translate_text(text: str, target_lang: str) -> Optional[str]:
    if not GOOGLE_API_KEY:
        print("[TRANSLATE ERROR] GOOGLE_API_KEY missing")
        return None

    payload = {
        "q": text,
        "target": target_lang,
        "format": "text",
        "key": GOOGLE_API_KEY,
    }

    try:
        response = requests.post(GOOGLE_TRANSLATE_URL, data=payload, timeout=15)
        print(f"[TRANSLATE] status={response.status_code}")

        if response.status_code != 200:
            print(f"[TRANSLATE ERROR] body={response.text}")
            return None

        data = response.json()
        return data["data"]["translations"][0]["translatedText"]

    except Exception as exc:
        print(f"[TRANSLATE ERROR] {str(exc)}")
        return None


# =========================================================
# WEBHOOK
# =========================================================
@app.route("/", methods=["GET"])
def home():
    return f"{APP_VERSION} LIVE", 200


@app.route("/webhook", methods=["POST"])
def callback():
    signature = request.headers.get("X-Line-Signature", "")
    body = request.get_data(as_text=True)

    print(f"[WEBHOOK] signature_exists={bool(signature)}")
    print(f"[WEBHOOK] body={body}")

    try:
        handler.handle(body, signature)
    except InvalidSignatureError:
        print("[WEBHOOK ERROR] invalid signature")
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
    group_id = normalize_id(getattr(event.source, "group_id", "") or "USER")
    text = safe_str(event.message.text)
    reply_token = event.reply_token

    if not text.startswith("/"):
        return False

    print(f"[COMMAND] raw={repr(text)} user_id={repr(user_id)} group_id={repr(group_id)}")

    if text.startswith("/grant"):
        if not is_user_admin(user_id):
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
        if not is_user_admin(user_id):
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

    if text.startswith("/lang"):
        parts = text.split()
        print(f"[COMMAND] /lang parts={parts}")

        if len(parts) != 2:
            reply_text(reply_token, "Cú pháp: /lang zh")
            return True

        target_lang = normalize_target_lang(parts[1])
        if not target_lang:
            reply_text(reply_token, "Ngôn ngữ không hỗ trợ. Dùng: zh, en, vi, ja, ko, th, id")
            return True

        success = save_user_target_lang(user_id, target_lang, group_id=group_id)
        if success:
            reply_text(reply_token, f"Đã lưu ngôn ngữ: {target_lang}")
        else:
            reply_text(reply_token, "Lưu ngôn ngữ thất bại")
        return True

    short_lang_map = {
        "/zh": "zh-TW",
        "/en": "en",
        "/vi": "vi",
        "/ja": "ja",
    }

    if text in short_lang_map:
        target_lang = short_lang_map[text]
        success = save_user_target_lang(user_id, target_lang, group_id=group_id)
        if success:
            reply_text(reply_token, f"Đã lưu ngôn ngữ: {target_lang}")
        else:
            reply_text(reply_token, "Lưu ngôn ngữ thất bại")
        return True

    return False


# =========================================================
# MAIN FLOW
# =========================================================
@handler.add(MessageEvent, message=TextMessage)
def handle_message(event):
    user_id = normalize_id(event.source.user_id)
    text = safe_str(event.message.text)

    print(f"[EVENT] user_id={repr(user_id)} text={repr(text)}")

    # Chặn command tuyệt đối ở tầng cao nhất
    if process_commands(event):
        print("[FLOW] command handled -> stop")
        return

    # Chỉ dịch khi không phải command
    target_lang = get_user_target_lang(user_id, default_lang="en")
    translated = translate_text(text, target_lang)

    if translated is None:
        reply_text(event.reply_token, "Dịch thất bại")
        return

    reply_text(event.reply_token, f"[AUTO -> {target_lang}]\n{translated}")


# =========================================================
# MAIN
# =========================================================
if __name__ == "__main__":
    port = int(os.getenv("PORT", "10000"))
    print(f"[BOOT] Running on 0.0.0.0:{port}")
    app.run(host="0.0.0.0", port=port)
