# =========================================================
# IMPORT
# =========================================================
import os
import json
import hmac
import base64
import hashlib
import html
import time
from datetime import datetime, timezone

import requests
import gspread
from flask import Flask, request, jsonify
from oauth2client.service_account import ServiceAccountCredentials

# =========================================================
# APP INIT
# =========================================================
app = Flask(__name__)

# =========================================================
# ENVIRONMENT VARIABLES
# =========================================================
LINE_CHANNEL_ACCESS_TOKEN = (os.getenv("LINE_CHANNEL_ACCESS_TOKEN") or "").strip()
LINE_CHANNEL_SECRET = (os.getenv("LINE_CHANNEL_SECRET") or "").strip()
GOOGLE_API_KEY = (os.getenv("GOOGLE_API_KEY") or "").strip()

GOOGLE_SHEET_ID = (
    os.getenv("GOOGLE_SHEET_ID")
    or os.getenv("SPREADSHEET_ID")
    or ""
).strip()

GOOGLE_CREDENTIALS_JSON = (
    os.getenv("GOOGLE_CREDENTIALS_JSON")
    or os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON")
    or ""
).strip()

# =========================================================
# CONSTANTS
# =========================================================
LINE_REPLY_URL = "https://api.line.me/v2/bot/message/reply"
GOOGLE_TRANSLATE_URL = "https://translation.googleapis.com/language/translate/v2"

USER_LANG_SHEET_NAME = "USER_LANG_MAP"
TRANSLATION_LOG_SHEET_NAME = "TRANSLATION_LOG"
USAGE_LOG_SHEET_NAME = "USAGE_LOG"

FREE_USAGE_LIMIT = 50
GROUP_DAILY_LIMIT = 1000
MAX_TEXT_LENGTH = 300

# ===== ANTI-SPAM MEMORY =====
LAST_MESSAGE_TIME = {}
COOLDOWN_SECONDS = 2

USER_LANG_HEADERS = [
    "user_id",
    "target_lang",
    "updated_at",
    "is_premium",
    "usage_count",
    "group_id",
    "role",
]

# =========================================================
# BOOT LOGS
# =========================================================
print("[BOOT] Starting LINE bot on Render.")
print(f"[BOOT] LINE_CHANNEL_ACCESS_TOKEN exists: {bool(LINE_CHANNEL_ACCESS_TOKEN)}")
print(f"[BOOT] LINE_CHANNEL_SECRET exists: {bool(LINE_CHANNEL_SECRET)}")
print(f"[BOOT] GOOGLE_API_KEY exists: {bool(GOOGLE_API_KEY)}")
print(f"[BOOT] GOOGLE_SHEET_ID exists: {bool(GOOGLE_SHEET_ID)}")
print(f"[BOOT] GOOGLE_CREDENTIALS_JSON exists: {bool(GOOGLE_CREDENTIALS_JSON)}")

# =========================================================
# ROOT
# =========================================================
@app.route("/", methods=["GET"])
def home():
    return "LINE webhook is live", 200


@app.route("/health", methods=["GET"])
def health():
    return jsonify({
        "status": "ok",
        "line_token_exists": bool(LINE_CHANNEL_ACCESS_TOKEN),
        "line_secret_exists": bool(LINE_CHANNEL_SECRET),
        "google_api_key_exists": bool(GOOGLE_API_KEY),
        "google_sheet_id_exists": bool(GOOGLE_SHEET_ID),
        "google_credentials_exists": bool(GOOGLE_CREDENTIALS_JSON),
    }), 200


# =========================================================
# UTILS
# =========================================================
def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def clean_input_text(text: str) -> str:
    clean_text = (text or "").strip()

    if "→" in clean_text:
        clean_text = clean_text.split("→")[0].strip()

    return clean_text


def normalize_target_lang(raw_lang: str):
    lang = (raw_lang or "").strip().lower()

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

    return mapping.get(lang)


def safe_str(value) -> str:
    return str(value or "").strip()


def safe_int(value, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return default


# =========================================================
# SECURITY
# =========================================================
def verify_signature(channel_secret: str, body: str, x_line_signature: str) -> bool:
    if not channel_secret:
        print("[SECURITY] LINE_CHANNEL_SECRET missing")
        return False

    if not x_line_signature:
        print("[SECURITY] X-Line-Signature missing")
        return False

    digest = hmac.new(
        channel_secret.encode("utf-8"),
        body.encode("utf-8"),
        hashlib.sha256,
    ).digest()

    computed_signature = base64.b64encode(digest).decode("utf-8")
    is_valid = hmac.compare_digest(computed_signature, x_line_signature)
    print(f"[SECURITY] signature_valid={is_valid}")
    return is_valid


# =========================================================
# LINE REPLY
# =========================================================
def reply_line_message(reply_token: str, text: str) -> bool:
    if not LINE_CHANNEL_ACCESS_TOKEN:
        print("[LINE REPLY ERROR] LINE_CHANNEL_ACCESS_TOKEN missing")
        return False

    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {LINE_CHANNEL_ACCESS_TOKEN}",
    }

    payload = {
        "replyToken": reply_token,
        "messages": [
            {
                "type": "text",
                "text": text,
            }
        ],
    }

    print(f"[LINE REPLY DEBUG] payload={json.dumps(payload, ensure_ascii=False)}")

    try:
        response = requests.post(
            LINE_REPLY_URL,
            headers=headers,
            json=payload,
            timeout=15,
        )
        print(f"[LINE REPLY] status={response.status_code}")
        print(f"[LINE REPLY] body={response.text}")
        return response.status_code == 200
    except Exception as exc:
        print(f"[LINE REPLY ERROR] {str(exc)}")
        return False


# =========================================================
# GOOGLE SHEET AUTH
# =========================================================
def get_gspread_client():
    if not GOOGLE_CREDENTIALS_JSON:
        print("[SHEET] GOOGLE_CREDENTIALS_JSON missing")
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

        client = gspread.authorize(credentials)
        return client

    except Exception as exc:
        print(f"[SHEET ERROR] authorize failed: {str(exc)}")
        return None


def get_spreadsheet():
    if not GOOGLE_SHEET_ID:
        print("[SHEET] GOOGLE_SHEET_ID missing")
        return None

    client = get_gspread_client()
    if client is None:
        return None

    try:
        return client.open_by_key(GOOGLE_SHEET_ID)
    except Exception as exc:
        print(f"[SHEET ERROR] open spreadsheet failed: {str(exc)}")
        return None


def get_user_lang_worksheet():
    spreadsheet = get_spreadsheet()
    if spreadsheet is None:
        return None

    try:
        return spreadsheet.worksheet(USER_LANG_SHEET_NAME)
    except Exception as exc:
        print(f"[SHEET ERROR] open USER_LANG_MAP failed: {str(exc)}")
        return None


def get_translation_log_worksheet():
    spreadsheet = get_spreadsheet()
    if spreadsheet is None:
        return None

    try:
        return spreadsheet.worksheet(TRANSLATION_LOG_SHEET_NAME)
    except Exception as exc:
        print(f"[SHEET ERROR] open TRANSLATION_LOG failed: {str(exc)}")
        return None


def get_usage_log_worksheet():
    spreadsheet = get_spreadsheet()
    if spreadsheet is None:
        return None

    try:
        return spreadsheet.worksheet(USAGE_LOG_SHEET_NAME)
    except Exception as exc:
        print(f"[SHEET ERROR] open USAGE_LOG failed: {str(exc)}")
        return None


# =========================================================
# USER_LANG_MAP HELPERS
# SCHEMA:
# user_id | target_lang | updated_at | is_premium | usage_count | group_id | role
# =========================================================
def ensure_user_lang_headers(worksheet) -> bool:
    if worksheet is None:
        return False

    try:
        values = worksheet.get_all_values()

        if not values:
            worksheet.append_row(USER_LANG_HEADERS)
            print("[SHEET] USER_LANG_MAP header created")
            return True

        return True

    except Exception as exc:
        print(f"[SHEET ERROR] ensure_user_lang_headers failed: {str(exc)}")
        return False


def get_user_lang_values(worksheet):
    if worksheet is None:
        return []

    try:
        return worksheet.get_all_values()
    except Exception as exc:
        print(f"[SHEET ERROR] get_all_values failed: {str(exc)}")
        return []


def find_user_row_index(values, user_id: str):
    for idx, row in enumerate(values[1:], start=2):
        current_user_id = row[0].strip() if len(row) > 0 else ""
        if current_user_id == safe_str(user_id):
            return idx
    return None


def build_user_row(
    user_id: str,
    target_lang: str = "en",
    updated_at: str = "",
    is_premium: str = "FALSE",
    usage_count: str = "0",
    group_id: str = "USER",
    role: str = "",
):
    return [
        safe_str(user_id),
        safe_str(target_lang) or "en",
        safe_str(updated_at) or now_iso(),
        safe_str(is_premium).upper() or "FALSE",
        safe_str(usage_count) or "0",
        safe_str(group_id) or "USER",
        safe_str(role).lower(),
    ]


def upsert_user_profile(
    user_id: str,
    target_lang: str = None,
    group_id: str = None,
    role: str = None,
) -> bool:
    worksheet = get_user_lang_worksheet()
    if worksheet is None:
        return False

    try:
        if not ensure_user_lang_headers(worksheet):
            return False

        values = get_user_lang_values(worksheet)
        if not values:
            return False

        found_row_index = find_user_row_index(values, user_id)
        timestamp = now_iso()

        if found_row_index:
            current_row = values[found_row_index - 1]

            current_target_lang = current_row[1].strip() if len(current_row) > 1 else "en"
            current_is_premium = current_row[3].strip() if len(current_row) > 3 else "FALSE"
            current_usage_count = current_row[4].strip() if len(current_row) > 4 else "0"
            current_group_id = current_row[5].strip() if len(current_row) > 5 else "USER"
            current_role = current_row[6].strip() if len(current_row) > 6 else ""

            new_row = build_user_row(
                user_id=user_id,
                target_lang=target_lang or current_target_lang or "en",
                updated_at=timestamp,
                is_premium=current_is_premium or "FALSE",
                usage_count=current_usage_count or "0",
                group_id=group_id or current_group_id or "USER",
                role=role if role is not None else current_role,
            )

            worksheet.update(f"A{found_row_index}:G{found_row_index}", [new_row])
            print(
                f"[SHEET] updated profile "
                f"user_id={user_id} "
                f"target_lang={new_row[1]} "
                f"group_id={new_row[5]} "
                f"role={new_row[6]}"
            )
            return True

        new_row = build_user_row(
            user_id=user_id,
            target_lang=target_lang or "en",
            updated_at=timestamp,
            is_premium="FALSE",
            usage_count="0",
            group_id=group_id or "USER",
            role=role or "",
        )
        worksheet.append_row(new_row)
        print(
            f"[SHEET] appended profile "
            f"user_id={user_id} "
            f"target_lang={new_row[1]} "
            f"group_id={new_row[5]} "
            f"role={new_row[6]}"
        )
        return True

    except Exception as exc:
        print(f"[SHEET ERROR] upsert_user_profile failed: {str(exc)}")
        return False


def get_user_target_lang(user_id: str, default_lang: str = "en") -> str:
    worksheet = get_user_lang_worksheet()
    if worksheet is None:
        print(f"[SHEET] fallback target_lang={default_lang}")
        return default_lang

    try:
        records = worksheet.get_all_records()

        for row in records:
            row_user_id = safe_str(row.get("user_id"))
            if row_user_id == safe_str(user_id):
                target_lang = safe_str(row.get("target_lang"))
                if target_lang:
                    print(f"[SHEET] found target_lang={target_lang} for user_id={user_id}")
                    return target_lang

        print(f"[SHEET] user_id not found, fallback target_lang={default_lang}")
        return default_lang

    except Exception as exc:
        print(f"[SHEET ERROR] get_user_target_lang failed: {str(exc)}")
        return default_lang


def save_user_target_lang(user_id: str, target_lang: str, group_id: str = "USER") -> bool:
    return upsert_user_profile(
        user_id=user_id,
        target_lang=target_lang,
        group_id=group_id,
    )


def increase_usage(user_id: str, group_id: str = "USER") -> int:
    worksheet = get_user_lang_worksheet()
    if worksheet is None:
        return 0

    try:
        if not ensure_user_lang_headers(worksheet):
            return 0

        values = get_user_lang_values(worksheet)
        if not values:
            return 0

        found_row_index = find_user_row_index(values, user_id)

        if not found_row_index:
            created = upsert_user_profile(
                user_id=user_id,
                target_lang="en",
                group_id=group_id,
            )
            print(f"[USAGE] user_id not found, created_profile={created}")
            values = get_user_lang_values(worksheet)
            found_row_index = find_user_row_index(values, user_id)

        if not found_row_index:
            return 0

        current_row = values[found_row_index - 1]
        current_target_lang = current_row[1].strip() if len(current_row) > 1 else "en"
        current_is_premium = current_row[3].strip() if len(current_row) > 3 else "FALSE"
        current_usage_count = current_row[4].strip() if len(current_row) > 4 else "0"
        current_group_id = current_row[5].strip() if len(current_row) > 5 else "USER"
        current_role = current_row[6].strip() if len(current_row) > 6 else ""

        new_usage_count = safe_int(current_usage_count, 0) + 1

        new_row = build_user_row(
            user_id=user_id,
            target_lang=current_target_lang or "en",
            updated_at=now_iso(),
            is_premium=current_is_premium or "FALSE",
            usage_count=str(new_usage_count),
            group_id=group_id or current_group_id or "USER",
            role=current_role,
        )

        worksheet.update(f"A{found_row_index}:G{found_row_index}", [new_row])
        print(f"[USAGE] user_id={user_id} usage_count={new_usage_count} group_id={new_row[5]}")
        return new_usage_count

    except Exception as exc:
        print(f"[USAGE ERROR] {str(exc)}")
        return 0


def get_group_usage(group_id: str) -> int:
    worksheet = get_user_lang_worksheet()
    if worksheet is None:
        return 0

    group_usage = 0

    try:
        values = worksheet.get_all_values()

        for row in values[1:]:
            if len(row) >= 6:
                row_group_id = row[5].strip()
                row_usage = safe_int(row[4], 0)

                if row_group_id == group_id:
                    group_usage += row_usage

        return group_usage

    except Exception as exc:
        print(f"[GROUP GUARD ERROR] {str(exc)}")
        return 0


def is_user_premium(user_id: str) -> bool:
    worksheet = get_user_lang_worksheet()
    if worksheet is None:
        return False

    try:
        records = worksheet.get_all_records()

        for row in records:
            if safe_str(row.get("user_id")) == safe_str(user_id):
                value = safe_str(row.get("is_premium")).upper()
                result = value == "TRUE"
                print(f"[PREMIUM] user_id={user_id} premium={result}")
                return result

        print(f"[PREMIUM] user_id={user_id} not found, premium=False")
        return False

    except Exception as exc:
        print(f"[PREMIUM ERROR] {str(exc)}")
        return False


def is_user_admin(user_id: str) -> bool:
    worksheet = get_user_lang_worksheet()
    if worksheet is None:
        return False

    try:
        records = worksheet.get_all_records()

        for row in records:
            if safe_str(row.get("user_id")) == safe_str(user_id):
                role = safe_str(row.get("role")).lower()
                result = role == "admin"
                print(f"[ADMIN] user_id={user_id} admin={result}")
                return result

        print(f"[ADMIN] user_id={user_id} not found, admin=False")
        return False

    except Exception as exc:
        print(f"[ADMIN ERROR] {str(exc)}")
        return False


def set_user_premium(user_id: str, premium: bool) -> bool:
    worksheet = get_user_lang_worksheet()
    if worksheet is None:
        return False

    try:
        if not ensure_user_lang_headers(worksheet):
            return False

        values = get_user_lang_values(worksheet)
        if not values:
            return False

        found_row_index = find_user_row_index(values, user_id)

        if not found_row_index:
            print(f"[PREMIUM SET] user_id not found: {user_id}")
            return False

        current_row = values[found_row_index - 1]

        current_target_lang = current_row[1].strip() if len(current_row) > 1 else "en"
        current_usage_count = current_row[4].strip() if len(current_row) > 4 else "0"
        current_group_id = current_row[5].strip() if len(current_row) > 5 else "USER"
        current_role = current_row[6].strip() if len(current_row) > 6 else ""

        premium_text = "TRUE" if premium else "FALSE"

        new_row = build_user_row(
            user_id=user_id,
            target_lang=current_target_lang or "en",
            updated_at=now_iso(),
            is_premium=premium_text,
            usage_count=current_usage_count or "0",
            group_id=current_group_id or "USER",
            role=current_role,
        )

        worksheet.update(f"A{found_row_index}:G{found_row_index}", [new_row])
        print(f"[PREMIUM SET] user_id={user_id} premium={premium_text}")
        return True

    except Exception as exc:
        print(f"[PREMIUM SET ERROR] {str(exc)}")
        return False


# =========================================================
# TRANSLATE
# =========================================================
def translate_text_with_meta(text: str, target_lang: str):
    if not GOOGLE_API_KEY:
        print("[TRANSLATE META] GOOGLE_API_KEY missing")
        return None, "unknown"

    payload = {
        "q": text,
        "target": target_lang,
        "format": "text",
        "key": GOOGLE_API_KEY,
    }

    print(f"[TRANSLATE META] input_text={text}")
    print(f"[TRANSLATE META] target_lang={target_lang}")

    try:
        response = requests.post(
            GOOGLE_TRANSLATE_URL,
            data=payload,
            timeout=20,
        )

        print(f"[TRANSLATE META] status={response.status_code}")
        print(f"[TRANSLATE META] body={response.text}")

        if response.status_code != 200:
            return None, "unknown"

        data = response.json()
        translation_item = data["data"]["translations"][0]

        translated = translation_item.get("translatedText", "")
        translated = html.unescape(translated)

        source_lang = translation_item.get("detectedSourceLanguage", "unknown")

        print(f"[TRANSLATE META] translated_text={translated}")
        print(f"[TRANSLATE META] detected_source_lang={source_lang}")

        return translated, source_lang

    except Exception as exc:
        print(f"[TRANSLATE META ERROR] {str(exc)}")
        return None, "unknown"


# =========================================================
# USAGE LOG
# =========================================================
def log_usage(user_id: str, message: str, source_lang: str, target_lang: str) -> bool:
    worksheet = get_usage_log_worksheet()
    if worksheet is None:
        print("[USAGE LOG] USAGE_LOG unavailable")
        return False

    try:
        values = worksheet.get_all_values()

        if not values:
            worksheet.append_row([
                "user_id",
                "message",
                "source_lang",
                "target_lang",
                "timestamp",
            ])

        worksheet.append_row([
            user_id or "",
            message or "",
            source_lang or "",
            target_lang or "",
            now_iso(),
        ])

        print(
            f"[USAGE LOG] saved "
            f"user_id={user_id} "
            f"source_lang={source_lang} "
            f"target_lang={target_lang}"
        )
        return True

    except Exception as exc:
        print(f"[USAGE LOG ERROR] {str(exc)}")
        return False


# =========================================================
# TRANSLATION LOG
# =========================================================
def log_translation_event(
    user_id: str,
    source_type: str,
    group_id: str,
    room_id: str,
    target_lang: str,
    input_text: str,
) -> bool:
    worksheet = get_translation_log_worksheet()
    if worksheet is None:
        return False

    try:
        values = worksheet.get_all_values()

        if not values:
            worksheet.append_row([
                "timestamp",
                "user_id",
                "source_type",
                "group_id",
                "room_id",
                "target_lang",
                "input_text",
            ])

        worksheet.append_row([
            now_iso(),
            user_id or "",
            source_type or "",
            group_id or "",
            room_id or "",
            target_lang or "",
            input_text or "",
        ])

        print(f"[LOG] translation event saved user_id={user_id}")
        return True

    except Exception as exc:
        print(f"[LOG ERROR] log_translation_event failed: {str(exc)}")
        return False


# =========================================================
# COMMANDS
# =========================================================
def handle_short_command(user_id: str, text: str, reply_token: str, group_id: str) -> bool:
    command_map = {
        "/zh": "zh-TW",
        "/en": "en",
        "/vi": "vi",
        "/ja": "ja",
        "/ko": "ko",
        "/th": "th",
        "/id": "id",
    }

    command = (text or "").strip().lower()

    if command not in command_map:
        return False

    target_lang = command_map[command]
    saved = save_user_target_lang(user_id, target_lang, group_id=group_id)

    if saved:
        usage_saved = log_usage(
            user_id=user_id,
            message=text,
            source_lang="command",
            target_lang=target_lang,
        )
        print(f"[USAGE LOG] short_command_saved={usage_saved}")

        ok = reply_line_message(reply_token, f"Đã lưu ngôn ngữ: {target_lang}")
        print(f"[REPLY DEBUG] short command result={ok}")
    else:
        ok = reply_line_message(reply_token, "Lưu ngôn ngữ thất bại. Kiểm tra kết nối Google Sheet.")
        print(f"[REPLY DEBUG] short command fail result={ok}")

    return True


def handle_lang_command(user_id: str, text: str, reply_token: str, group_id: str):
    parts = (text or "").strip().split()

    if len(parts) != 2:
        ok = reply_line_message(reply_token, "Cú pháp đúng: /lang zh")
        print(f"[REPLY DEBUG] lang syntax fail result={ok}")
        return

    raw_lang = parts[1]
    target_lang = normalize_target_lang(raw_lang)

    if not target_lang:
        ok = reply_line_message(reply_token, "Ngôn ngữ không hỗ trợ. Dùng: zh, en, vi, ja, ko, th, id")
        print(f"[REPLY DEBUG] lang invalid result={ok}")
        return

    saved = save_user_target_lang(user_id, target_lang, group_id=group_id)

    if saved:
        usage_saved = log_usage(
            user_id=user_id,
            message=text,
            source_lang="command",
            target_lang=target_lang,
        )
        print(f"[USAGE LOG] lang_command_saved={usage_saved}")

        ok = reply_line_message(reply_token, f"Đã lưu ngôn ngữ: {target_lang}")
        print(f"[REPLY DEBUG] lang command result={ok}")
    else:
        ok = reply_line_message(reply_token, "Lưu ngôn ngữ thất bại. Kiểm tra kết nối Google Sheet.")
        print(f"[REPLY DEBUG] lang command fail result={ok}")


def handle_upgrade_command(user_id: str, reply_token: str) -> bool:
    premium = is_user_premium(user_id)

    if premium:
        ok = reply_line_message(
            reply_token,
            "Bạn đang ở gói Premium. Nếu cần hỗ trợ thêm, hãy liên hệ admin."
        )
        print(f"[REPLY DEBUG] upgrade already premium result={ok}")
        return True

    upgrade_text = (
        "Nâng cấp Premium:\n"
        "- Bỏ giới hạn miễn phí\n"
        "- Ưu tiên hỗ trợ nhóm\n"
        "- Liên hệ admin để kích hoạt"
    )
    ok = reply_line_message(reply_token, upgrade_text)
    print(f"[REPLY DEBUG] upgrade command result={ok}")
    return True


def handle_grant_command(user_id: str, text: str, reply_token: str) -> bool:
    command_text = (text or "").strip()

    if not command_text.lower().startswith("/grant"):
        return False

    print(f"[DEBUG GRANT] text={command_text}")

    if not is_user_admin(user_id):
        print("[DEBUG GRANT] not admin")
        reply_line_message(reply_token, "Bạn không có quyền admin.")
        return True

    parts = command_text.split()

    if len(parts) != 2:
        print("[DEBUG GRANT] syntax error")
        reply_line_message(reply_token, "Cú pháp: /grant USER_ID")
        return True

    target_user_id = parts[1].strip()
    print(f"[DEBUG GRANT] target={target_user_id}")

    success = set_user_premium(target_user_id, True)
    print(f"[DEBUG GRANT] success={success}")

    if success:
        reply_line_message(reply_token, f"Đã cấp premium cho {target_user_id}")
    else:
        reply_line_message(reply_token, "Cấp premium thất bại")

    return True


def handle_revoke_command(user_id: str, text: str, reply_token: str) -> bool:
    command_text = (text or "").strip()

    if not command_text.lower().startswith("/revoke"):
        return False

    if not is_user_admin(user_id):
        reply_line_message(reply_token, "Bạn không có quyền admin.")
        return True

    parts = command_text.split()

    if len(parts) != 2:
        reply_line_message(reply_token, "Cú pháp: /revoke USER_ID")
        return True

    target_user_id = parts[1].strip()
    success = set_user_premium(target_user_id, False)

    if success:
        reply_line_message(reply_token, f"Đã gỡ premium cho {target_user_id}")
    else:
        reply_line_message(reply_token, "Gỡ premium thất bại")

    return True


# =========================================================
# NORMAL MESSAGE FLOW
# =========================================================
def handle_normal_message(
    user_id: str,
    text: str,
    reply_token: str,
    source_type: str,
    group_id: str,
    room_id: str,
):
    print(f"[MESSAGE FLOW] raw_input_text={text}")
    clean_text = clean_input_text(text)
    print(f"[MESSAGE FLOW] clean_input_text={clean_text}")

    # ===== COOLDOWN GUARD =====
    current_time = time.time()
    last_time = LAST_MESSAGE_TIME.get(user_id, 0)

    if current_time - last_time < COOLDOWN_SECONDS:
        print(f"[COOLDOWN BLOCK] user_id={user_id}")
        reply_line_message(reply_token, f"Bạn gửi quá nhanh, vui lòng đợi {COOLDOWN_SECONDS} giây.")
        return

    LAST_MESSAGE_TIME[user_id] = current_time

    # ===== COST GUARD LAYER =====
    if len(clean_text) > MAX_TEXT_LENGTH:
        reply_line_message(reply_token, f"Tin nhắn quá dài (>{MAX_TEXT_LENGTH} ký tự)")
        print(f"[GUARD] blocked long text len={len(clean_text)}")
        return

    if clean_text.strip() == "":
        reply_line_message(reply_token, "Tin nhắn không hợp lệ")
        print("[GUARD] blocked empty spam")
        return

    if not clean_text:
        ok = reply_line_message(reply_token, "Tin nhắn trống.")
        print(f"[REPLY DEBUG] empty text result={ok}")
        return

    profile_saved = upsert_user_profile(user_id=user_id, group_id=group_id)
    print(f"[PROFILE] upsert_before_translate={profile_saved}")

    target_lang = get_user_target_lang(user_id, default_lang="en")
    print(f"[MESSAGE FLOW] target_lang={target_lang}")

    usage = increase_usage(user_id, group_id=group_id)
    group_usage = get_group_usage(group_id)
    premium = is_user_premium(user_id)

    print(f"[GROUP GUARD] group_id={group_id} usage={group_usage}")
    print(f"[LIMIT] usage={usage} premium={premium}")

    if group_usage > GROUP_DAILY_LIMIT:
        reply_line_message(
            reply_token,
            "Nhóm đã vượt giới hạn sử dụng hôm nay. Liên hệ admin để nâng cấp."
        )
        print(f"[GROUP GUARD] BLOCKED group_id={group_id}")
        return

    if not premium and usage > FREE_USAGE_LIMIT:
        ok = reply_line_message(
            reply_token,
            f"Bạn đã vượt giới hạn miễn phí ({FREE_USAGE_LIMIT} lần). Liên hệ admin để nâng cấp."
        )
        print(f"[REPLY DEBUG] free limit blocked result={ok}")
        return

    translated, source_lang = translate_text_with_meta(clean_text, target_lang)

    if translated is None:
        usage_saved = log_usage(
            user_id=user_id,
            message=clean_text,
            source_lang="unknown",
            target_lang=target_lang,
        )
        print(f"[USAGE LOG] saved_on_translate_fail={usage_saved}")

        ok = reply_line_message(
            reply_token,
            "Dịch thất bại. Kiểm tra GOOGLE_API_KEY hoặc Google Sheet credentials."
        )
        print(f"[REPLY DEBUG] translate failed result={ok}")
        return

    usage_saved = log_usage(
        user_id=user_id,
        message=clean_text,
        source_lang=source_lang,
        target_lang=target_lang,
    )
    print(f"[USAGE LOG] usage_saved={usage_saved}")

    log_saved = log_translation_event(
        user_id=user_id,
        source_type=source_type,
        group_id=group_id,
        room_id=room_id,
        target_lang=target_lang,
        input_text=clean_text,
    )
    print(f"[LOG] translation_log_saved={log_saved}")

    output_text = f"[AUTO → {target_lang}]\n{translated}"
    ok = reply_line_message(reply_token, output_text)
    print(f"[REPLY DEBUG] normal success result={ok}")


# =========================================================
# WEBHOOK
# =========================================================
@app.route("/webhook", methods=["POST"])
def webhook():
    print("=== NEW REQUEST RECEIVED ===")

    body = request.get_data(as_text=True)
    print(f"[WEBHOOK RAW BODY] {body}")

    x_line_signature = request.headers.get("X-Line-Signature", "")
    print(f"[WEBHOOK HEADER] x_line_signature_exists={bool(x_line_signature)}")

    if not verify_signature(LINE_CHANNEL_SECRET, body, x_line_signature):
        return jsonify({"ok": False, "error": "invalid signature"}), 400

    try:
        data = request.get_json(force=True)
        print("[WEBHOOK PARSED]")
        print(json.dumps(data, ensure_ascii=False))
    except Exception as exc:
        print(f"[WEBHOOK JSON ERROR] {str(exc)}")
        return jsonify({"ok": False, "error": "invalid json"}), 400

    events = data.get("events", [])
    print(f"[WEBHOOK] events_count={len(events)}")

    for event in events:
        event_type = event.get("type")
        source = event.get("source", {})
        message = event.get("message", {})
        reply_token = event.get("replyToken")

        user_id = source.get("userId")
        group_id = source.get("groupId") or source.get("roomId") or "USER"
        room_id = source.get("roomId")
        source_type = source.get("type")
        message_type = message.get("type")
        text = (message.get("text") or "").strip()

        print(
            f"[EVENT] "
            f'{{"event_type":"{event_type}",'
            f'"reply_token_exists":{bool(reply_token)},'
            f'"source_type":"{source_type}",'
            f'"user_id":"{user_id}",'
            f'"group_id":"{group_id}",'
            f'"room_id":"{room_id}",'
            f'"message_type":"{message_type}",'
            f'"text":"{text}"}}'
        )

        if event_type != "message":
            continue

        if message_type != "text":
            continue

        if not user_id:
            print("[MESSAGE] user_id missing")
            if reply_token:
                reply_line_message(reply_token, "Không lấy được user_id từ LINE event.")
            continue

        print(f"[MESSAGE] source_type={source_type}")
        print(f"[MESSAGE] group_id={group_id}")
        print(f"[MESSAGE] room_id={room_id}")
        print(f"[MESSAGE] user_id={user_id}")
        print(f"[MESSAGE] text={text}")

        # COMMAND FLOW
        if handle_short_command(user_id, text, reply_token, group_id=group_id):
            continue

        if text.startswith("/lang"):
            handle_lang_command(user_id, text, reply_token, group_id=group_id)
            continue

        if text.startswith("/upgrade"):
            handle_upgrade_command(user_id, reply_token)
            continue

        if handle_grant_command(user_id, text, reply_token):
            continue

        if handle_revoke_command(user_id, text, reply_token):
            continue

        # NORMAL FLOW
        handle_normal_message(
            user_id=user_id,
            text=text,
            reply_token=reply_token,
            source_type=source_type,
            group_id=group_id,
            room_id=room_id,
        )

    return jsonify({"ok": True}), 200


# =========================================================
# MAIN
# =========================================================
if __name__ == "__main__":
    port = int(os.getenv("PORT", "10000"))
    print(f"[BOOT] Running on 0.0.0.0:{port}")
    app.run(host="0.0.0.0", port=port, debug=False)
