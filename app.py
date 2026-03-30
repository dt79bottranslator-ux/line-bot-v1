import os
import json
import requests
from flask import Flask, request, jsonify
from google.oauth2 import service_account
from google.auth.transport.requests import Request

app = Flask(__name__)

LINE_CHANNEL_ACCESS_TOKEN = os.getenv("LINE_CHANNEL_ACCESS_TOKEN", "").strip()
GOOGLE_SERVICE_ACCOUNT_JSON = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", "").strip()


def get_google_access_token():
    info = json.loads(GOOGLE_SERVICE_ACCOUNT_JSON)

    credentials = service_account.Credentials.from_service_account_info(
        info,
        scopes=["https://www.googleapis.com/auth/cloud-platform"]
    )

    credentials.refresh(Request())
    return credentials.token


def translate_text(text, target_language="vi"):
    if not text:
        return ""

    access_token = get_google_access_token()

    url = "https://translation.googleapis.com/v3/projects/dt79-bot-system/locations/global:translateText"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }
    payload = {
        "contents": [text],
        "mimeType": "text/plain",
        "targetLanguageCode": target_language
    }

    response = requests.post(url, headers=headers, json=payload, timeout=20)

    if response.status_code != 200:
        return f"[Translate API Error] {response.status_code}: {response.text}"

    data = response.json()
    return data["translations"][0]["translatedText"]


def reply_text(reply_token, text):
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
                "text": text
            }
        ]
    }

    response = requests.post(url, headers=headers, json=payload, timeout=10)
    return response.status_code, response.text


@app.route("/", methods=["GET"])
def home():
    return "LINE BOT OK", 200


@app.route("/webhook", methods=["POST"])
def webhook():
    body = request.get_json(silent=True)

    if not body:
        return jsonify({"status": "no json body"}), 200

    if "events" not in body:
        return jsonify({"status": "no events"}), 200

    for event in body["events"]:
        if event.get("type") != "message":
            continue

        message = event.get("message", {})
        if message.get("type") != "text":
            continue

        user_text = message.get("text", "")
        reply_token = event.get("replyToken")

        translated = translate_text(user_text, target_language="vi")
        reply_text(reply_token, translated)

    return jsonify({"status": "ok"}), 200


if __name__ == "__main__":
    port = int(os.getenv("PORT", 10000))
    app.run(host="0.0.0.0", port=port)