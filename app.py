import os
import json
import logging
import requests
from flask import Flask, request, jsonify

import gspread
from oauth2client.service_account import ServiceAccountCredentials

from google.oauth2 import service_account
from google.auth.transport.requests import Request

app = Flask(__name__)

# =========================================================
# LOGGING
# =========================================================
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# =========================================================
# ENV VARIABLES
# =========================================================
LINE_CHANNEL_ACCESS_TOKEN = os.getenv("LINE_CHANNEL_ACCESS_TOKEN", "").strip()
GOOGLE_SERVICE_ACCOUNT_JSON = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", "").strip()
PORT = int(os.getenv("PORT", "10000"))

# ❗❗❗ QUAN TRỌNG: DÁN GOOGLE SHEET ID VÀO ĐÂY ❗❗❗
SPREADSHEET_ID = "1XE4CLi8bqwHux-2vGjYNMH5iDKVmOj8c8fx-Cyk5Upg"

# =========================================================
# GOOGLE TRANSLATE CONFIG
# =========================================================
PROJECT_ID = "dt79-bot-system"
TRANSLATE_URL = f"https://translation.googleapis.com/v3/projects/{PROJECT_ID}/locations/global:translateText"

# =========================================================
# GOOGLE SHEET CONNECT
# =========================================================
def connect_sheet():
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive"
    ]

    creds_dict = json.loads(GOOGLE_SERVICE_ACCOUNT_JSON)
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)

    client = gspread.authorize(creds)

    sheet = client.open_by_key(SPREADSHEET_ID)
    return sheet.worksheet("USER_LANG_MAP")


# =========================================================
# UPSERT USER LANGUAGE
# =========================================================
def upsert_user_language(user_id, target_lang):
    sheet = connect_sheet()
    records = sheet.get_all_records()

    for idx, row in enumerate(records):
        if row["user_id"] == user_id:
            sheet.update_cell(idx + 2, 2, target_lang)
            sheet.update_cell(idx + 2, 3, get_timestamp())
            logger.info("Updated user_id=%s", user_id)
            return

    sheet.append_row([user_id, target_lang, get_timestamp()])
    logger.info("Inserted user_id=%s", user_id)


def get_user_language(user_id):
    sheet = connect_sheet()
    records = sheet.get_all_records()

    for row in records:
        if row["user_id"] == user_id:
            return row["target_lang"]

    return "vi"


def get_timestamp():
    from datetime import datetime
    return datetime.utcnow().isoformat()


# =========================================================
# GOOGLE TRANSLATE
# =========================================================
def get_access_token():
    creds_dict = json.loads(GOOGLE_SERVICE_ACCOUNT_JSON)
    credentials = service_account.Credentials.from_service_account_info(
        creds_dict,
        scopes=["https://www.googleapis.com/auth/cloud-platform"]
    )
    credentials.refresh(Request())
    return credentials.token


def translate_text(text, target_lang):
    token = get_access_token()

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

    payload = {
        "contents": [text],
        "targetLanguageCode": target_lang
    }

    res = requests.post(TRANSLATE_URL, headers=headers, json=payload)

    if res.status_code != 200:
        raise Exception(res.text)

    return res.json()["translations"][0]["translatedText"]


# =========================================================
# LINE REPLY
# =========================================================
def reply(reply_token, text):
    url = "https://api.line.me/v2/bot/message/reply"

    headers = {
        "Authorization": f"Bearer {LINE_CHANNEL_ACCESS_TOKEN}",
        "Content-Type": "application/json"
    }

    payload = {
        "replyToken": reply_token,
        "messages": [{"type": "text", "text": text}]
    }

    requests.post(url, headers=headers, json=payload)


# =========================================================
# ROUTES
# =========================================================
@app.route("/", methods=["GET"])
def home():
    return "OK", 200


@app.route("/webhook", methods=["POST"])
def webhook():
    body = request.get_json()
    events = body.get("events", [])

    for event in events:
        try:
            if event["type"] != "message":
                continue

            if event["message"]["type"] != "text":
                continue

            text = event["message"]["text"]
            reply_token = event["replyToken"]
            user_id = event["source"]["userId"]

            logger.info("Incoming: %s", text)

            # COMMAND
            if text.startswith("/lang"):
                lang = text.split(" ")[1]

                if lang == "zh":
                    lang = "zh-TW"

                upsert_user_language(user_id, lang)

                reply(reply_token, f"Đã lưu ngôn ngữ: {lang}")
                continue

            # TRANSLATE
            target_lang = get_user_language(user_id)
            translated = translate_text(text, target_lang)

            reply(reply_token, translated)

        except Exception as e:
            logger.exception("Error")
            reply(reply_token, str(e))

    return jsonify({"status": "ok"}), 200


# =========================================================
# MAIN
# =========================================================
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=PORT)