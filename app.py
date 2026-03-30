from flask import Flask, request, jsonify
import requests
import os

app = Flask(__name__)

LINE_CHANNEL_ACCESS_TOKEN = os.getenv("LINE_CHANNEL_ACCESS_TOKEN")

def simple_translate(text):
    mapping = {
        "hello": "xin chào",
        "bye": "tạm biệt"
    }
    return mapping.get(text.lower(), f"(demo) {text}")

def reply_text(reply_token, text):
    url = "https://api.line.me/v2/bot/message/reply"

    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {LINE_CHANNEL_ACCESS_TOKEN}"
    }

    data = {
        "replyToken": reply_token,
        "messages": [
            {
                "type": "text",
                "text": text
            }
        ]
    }

    requests.post(url, headers=headers, json=data)

@app.route("/")
def home():
    return "Bot is running"

@app.route("/webhook", methods=["POST"])
def webhook():
    body = request.json

    if "events" not in body:
        return jsonify({"status": "no events"})

    for event in body["events"]:
        if event["type"] == "message":
            if event["message"]["type"] == "text":
                user_text = event["message"]["text"]
                reply_token = event["replyToken"]

                translated = simple_translate(user_text)

                reply_text(reply_token, translated)

    return jsonify({"status": "ok"})

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=10000)