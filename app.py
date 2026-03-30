import os
import json
import logging
import requests
from flask import Flask, request, jsonify
from google.oauth2 import service_account
from google.auth.transport.requests import Request

app = Flask(__name__)

# =========================================================
# LOGGING (nhật ký)
# =========================================================
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# =========================================================
# ENVIRONMENT VARIABLES (biến môi trường)
# =========================================================
LINE_CHANNEL_ACCESS_TOKEN = (os.getenv("LINE_CHANNEL_ACCESS_TOKEN") or "").strip()
GOOGLE_SERVICE_ACCOUNT_JSON = (os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON") or "").strip()
PORT = int(os.getenv("PORT", "10000"))

# =========================================================
# GOOGLE CLOUD CONFIG (cấu hình Google Cloud)
# =========================================================
GOOGLE_PROJECT_ID = "dt79-bot-system"
GOOGLE_TRANSLATE_ENDPOINT = (
    f"https://translation.googleapis.com/v3/projects/"
    f"{GOOGLE_PROJECT_ID}/locations/global:translateText"
)
GOOGLE_AUTH_SCOPE = ["https://www.googleapis.com/auth/cloud-platform"]

# =========================================================
# MULTI-LANGUAGE MODE (chế độ đa ngôn ngữ)
# user input (đầu vào người dùng) -> actual target language (ngôn ngữ đích thật)
# zh sẽ được map sang zh-TW để ra chữ phồn thể Đài Loan
# =========================================================
LANGUAGE_ALIAS_MAP = {
    "vi": "vi",
    "en": "en",
    "zh": "zh-TW",
    "zh-tw": "zh-TW",
}

SUPPORTED_LANG_COMMANDS = {"vi", "en", "zh", "zh-tw"}
DEFAULT_TARGET_LANG = "vi"

# In-memory store (bộ nhớ tạm trong RAM)
user_language_prefs = {}

# =========================================================
# VALIDATION (kiểm tra cấu hình)
# =========================================================
if not LINE_CHANNEL_ACCESS_TOKEN:
    raise RuntimeError("Thiếu LINE_CHANNEL_ACCESS_TOKEN trong environment variables")

if not GOOGLE_SERVICE_ACCOUNT_JSON:
    raise RuntimeError("Thiếu GOOGLE_SERVICE_ACCOUNT_JSON trong environment variables")


# =========================================================
# HELPERS (hàm phụ trợ)
# =========================================================
def get_google_access_token() -> str:
    """
    Tạo access token (mã truy cập) từ service account JSON
    """
    service_account_info = json.loads(GOOGLE_SERVICE_ACCOUNT_JSON)

    credentials = service_account.Credentials.from_service_account_info(
        service_account_info,
        scopes=GOOGLE_AUTH_SCOPE
    )

    credentials.refresh(Request())
    return credentials.token


def translate_text_with_google(text: str, target_lang: str) -> str:
    """
    Gọi Google Cloud Translation API để dịch văn bản
    """
    access_token = get_google_access_token()

    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json; charset=utf-8"
    }

    payload = {
        "contents": [text],
        "mimeType": "text/plain",
        "targetLanguageCode": target_lang
    }

    response = requests.post(
        GOOGLE_TRANSLATE_ENDPOINT,
        headers=headers,
        json=payload,
        timeout=30
    )

    logger.info("Translate status=%s target_lang=%s", response.status_code, target_lang)

    if response.status_code != 200:
        raise RuntimeError(
            f"Translate API failed: {response.status_code} | {response.text}"
        )

    data = response.json()
    translations = data.get("translations", [])

    if not translations:
        raise RuntimeError(f"Không có translations trong response: {data}")

    translated_text = translations[0].get("translatedText", "").strip()

    if not translated_text:
        raise RuntimeError(f"translatedText rỗng: {data}")

    return translated_text


def reply_to_line(reply_token: str, text: str) -> None:
    """
    Gửi reply về LINE
    """
    url = "https://api.line.me/v2/bot/message/reply"

    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {LINE_CHANNEL_ACCESS_TOKEN}"
    }

    payload = {
        "replyToken": reply_token,
        "messages": [
            {
                "type": "text",
                "text": text[:1000]
            }
        ]
    }

    response = requests.post(url, headers=headers, json=payload, timeout=30)

    logger.info("LINE reply status=%s", response.status_code)

    if response.status_code != 200:
        raise RuntimeError(
            f"LINE reply failed: {response.status_code} | {response.text}"
        )


def normalize_language_command(lang_input: str) -> str:
    """
    Chuẩn hóa lệnh ngôn ngữ người dùng nhập
    Ví dụ:
    zh -> zh-TW
    zh-tw -> zh-TW
    vi -> vi
    en -> en
    """
    normalized = lang_input.strip().lower()
    return LANGUAGE_ALIAS_MAP.get(normalized, "")


def handle_language_command(text: str, user_id: str) -> str:
    """
    Xử lý lệnh:
    /lang vi
    /lang en
    /lang zh
    /lang zh-TW
    """
    parts = text.strip().split()

    if len(parts) != 2:
        return "Sai cú pháp. Dùng: /lang vi hoặc /lang en hoặc /lang zh"

    lang_input = parts[1].strip().lower()

    if lang_input not in SUPPORTED_LANG_COMMANDS:
        return "Ngôn ngữ chưa hỗ trợ. Chỉ hỗ trợ: vi, en, zh"

    actual_target_lang = normalize_language_command(lang_input)
    if not actual_target_lang:
        return "Không chuẩn hóa được ngôn ngữ đích."

    user_language_prefs[user_id] = actual_target_lang

    logger.info(
        "Saved language user_id=%s input=%s target_lang=%s",
        user_id,
        lang_input,
        actual_target_lang
    )

    if actual_target_lang == "zh-TW":
        return "Đã lưu ngôn ngữ đích: zh-TW (Tiếng Trung phồn thể Đài Loan)"

    return f"Đã lưu ngôn ngữ đích: {actual_target_lang}"


def get_user_target_language(user_id: str) -> str:
    """
    Lấy ngôn ngữ đích theo user
    """
    return user_language_prefs.get(user_id, DEFAULT_TARGET_LANG)


# =========================================================
# ROUTES (đường dẫn)
# =========================================================
@app.route("/", methods=["GET"])
def home():
    return "LINE Bot is running", 200


@app.route("/webhook", methods=["POST"])
def webhook():
    """
    Webhook endpoint (điểm nhận webhook) từ LINE
    """
    body = request.get_json(silent=True)
    logger.info("Webhook hit")

    if not body:
        logger.warning("Empty body")
        return jsonify({"status": "ignored", "reason": "empty body"}), 200

    events = body.get("events", [])
    logger.info("Events count=%s", len(events))

    for event in events:
        reply_token = event.get("replyToken")

        try:
            event_type = event.get("type")
            logger.info("Event type=%s", event_type)

            if event_type != "message":
                continue

            message = event.get("message", {})
            message_type = message.get("type")
            logger.info("Message type=%s", message_type)

            if message_type != "text":
                continue

            if not reply_token:
                logger.warning("Missing replyToken")
                continue

            source = event.get("source", {})
            user_id = source.get("userId", "unknown")
            incoming_text = (message.get("text") or "").strip()

            logger.info("Incoming text=%s | user_id=%s", incoming_text, user_id)

            if not incoming_text:
                reply_to_line(reply_token, "Tin nhắn rỗng.")
                continue

            # =========================================
            # COMMAND MODE: /lang
            # =========================================
            if incoming_text.lower().startswith("/lang"):
                reply_text = handle_language_command(incoming_text, user_id)
                reply_to_line(reply_token, reply_text)
                continue

            # =========================================
            # NORMAL TRANSLATION MODE
            # =========================================
            target_lang = get_user_target_language(user_id)
            logger.info("Using target_lang=%s for user_id=%s", target_lang, user_id)

            translated_text = translate_text_with_google(incoming_text, target_lang)
            reply_to_line(reply_token, translated_text)

        except Exception as e:
            logger.exception("Webhook processing error")

            try:
                if reply_token:
                    reply_to_line(reply_token, f"Lỗi xử lý: {str(e)}")
            except Exception:
                logger.exception("Failed to send error reply to LINE")

    return jsonify({"status": "ok"}), 200


# =========================================================
# MAIN (điểm chạy chính)
# =========================================================
if __name__ == "__main__":
    logger.info("Starting app on port %s", PORT)
    app.run(host="0.0.0.0", port=PORT)