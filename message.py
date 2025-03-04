import json
import os
import traceback

import requests
from loguru import logger

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_BOT_CHAT_ID = os.getenv("TELEGRAM_BOT_CHAT_ID")
TELEGRAM_REPORT_CHAT_ID = os.getenv("TELEGRAM_REPORT_CHAT_ID")

def send_report(message):
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    error = f"{message}\n{traceback.format_exc()}"
    data = {
        'chat_id': TELEGRAM_REPORT_CHAT_ID,
        'text': error,
        'parse_mode': 'HTML',
    }
    requests.post(url, data=data)
    logger.error(error)


def send_telegram(message):
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    data = {
        'chat_id': TELEGRAM_BOT_CHAT_ID,
        'text': message,
        'parse_mode': 'HTML',
        'disable_web_page_preview': 'true'
    }
    response = requests.post(url, data=data)
    if response.status_code == 200:
        logger.success('Сообщение в Telegram отправлено')
    else:
        send_report(f'Ошибка при отправке сообщения в Telegram. Код статуса: {response.status_code}: {response.text}')


def send_telegram_video(video_path, message):
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendVideo"
    try:
        with open(video_path, 'rb') as video_file:
            data = {
                'chat_id': TELEGRAM_BOT_CHAT_ID,
                'caption': message,
                'supports_streaming': True,
                'parse_mode': 'HTML',
                'disable_web_page_preview': 'true'
            }
            files = {'video': video_file}
            response = requests.post(url, data=data, files=files)
            if response.status_code == 200:
                logger.success('Видео успешно отправлено в Telegram.')
            else:
                send_report(f'Ошибка при отправке видео. Код статуса: {response.status_code}, {response.text}')
    except FileNotFoundError:
        send_report('Файл видео не найден')


def send_telegram_videos(video_paths, message):
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMediaGroup"
    media, files = [], {}
    try:
        for idx, video_path in enumerate(video_paths):
            if os.path.exists(video_path):
                file_key = f"video{idx}"
                files[file_key] = (os.path.basename(video_path), open(video_path, "rb"), "video/mp4")
                media.append({
                    "type": "video",
                    "media": f"attach://{file_key}",
                    "supports_streaming": True,
                    'parse_mode': 'HTML',
                    'disable_web_page_preview': 'true'
                })

        if media:
            media[0]["caption"] = message  # Подпись добавляется к первому видео
            response = requests.post(url, data={"chat_id": TELEGRAM_BOT_CHAT_ID, "media": json.dumps(media)}, files=files)
            logger.success("Видео успешно отправлены." if response.ok else f"Ошибка: {response.status_code} - {response.text}")

        for file in files.values():
            file[1].close() # Закрываем файл
    except FileNotFoundError:
        send_report('Файл видео не найден')
