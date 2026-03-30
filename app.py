import os
import requests
from flask import Flask, request, jsonify

app = Flask(__name__)

LINE_CHANNEL_ACCESS_TOKEN = os.getenv("LINE_CHANNEL_ACCESS_TOKEN", "").strip()


def simple_translate(text):
    text = (text or "").lower().strip()

    if text == "hello":
        return "xin chào"
    elif text == "bye":
        return "tạm biệt"
    else:
        return f"(demo) {text}"


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

        translated = simple_translate(user_text)
        reply_text(reply_token, translated)

    return jsonify({"status": "ok"}), 200


if __name__ == "__main__":
    port = int(os.getenv("PORT", 10000))
    app.run(host="0.0.0.0", port=port)