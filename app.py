from flask import Flask, request, jsonify
import os
import requests

app = Flask(__name__)

LINE_CHANNEL_ACCESS_TOKEN = os.getenv("LINE_CHANNEL_ACCESS_TOKEN")

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

    r = requests.post(url, headers=headers, json=payload)
    print("LINE reply status:", r.status_code)
    print("LINE reply body:", r.text)
    return r


def simple_translate(text):
    text = text.strip().lower()

    mapping = {
        "hello": "xin chào",
        "bye": "tạm biệt"
    }

    return mapping.get(text, f"(demo) {text}")


@app.route("/")
def home():
    return "LINE Bot is running"


@app.route("/webhook", methods=["POST"])
def webhook():
    body = request.get_json()
    print("Incoming payload:", body)

    events = body.get("events", [])

    for event in events:
        if event.get("type") == "message":
            message = event.get("message", {})
            if message.get("type") == "text":
                user_text = message.get("text", "")
                reply_token = event.get("replyToken")

                translated_text = simple_translate(user_text)
                reply_text(reply_token, translated_text)

    return jsonify({"status": "ok"})