import os
import subprocess
import urllib.parse
from datetime import datetime, timedelta, timezone
from functools import wraps

from apscheduler.schedulers.blocking import BlockingScheduler
from dotenv import load_dotenv
from loguru import logger

load_dotenv()

import message
from database import db
from dictionaries import CATEGORY_URLS
from movie import Movie
from parcer import (check_movie_release, download_video,
                    get_top_movies_and_serials, get_youtube_links)

CATEGORY_BAN_LIST = os.getenv("CATEGORY_BAN_LIST").split()
COUNTRY_BAN_LIST = os.getenv("COUNTRY_BAN_LIST").split()

def time_spent(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        time_start = datetime.now()

        r = func(*args, **kwargs)

        time_end = datetime.now() - time_start
        hours = time_end.seconds // 3600
        minutes = (time_end.seconds % 3600) // 60
        seconds = time_end.seconds % 60
        milliseconds = time_end.microseconds // 1000
        if hours > 0:
            logger.info(f"{func.__name__} завершено за {hours} ч {minutes} мин {seconds} сек {milliseconds} мс")
        elif minutes > 0:
            logger.info(f"{func.__name__} завершено за {minutes} мин {seconds} сек {milliseconds} мс")
        elif seconds > 0:
            logger.info(f"{func.__name__} завершено за {seconds} сек {milliseconds} мс")
        else:
            logger.info(f"{func.__name__} завершено за {milliseconds} мс")
        return r
    return wrapper

@time_spent
def check_updates():
    # Шаг 1: Получить список устаревших пакетов
    result = subprocess.run(
        ["pip", "list", "--outdated", "--format=json"],
        capture_output=True,
        text=True
    )
    outdated_packages = eval(result.stdout)  # Преобразуем JSON-строку в список словарей

    # Шаг 2: Для каждого устаревшего пакета выполнить обновление
    for package in outdated_packages:
        package_name = package['name']
        if package_name == "pip":
            # Специальная команда для обновления pip
            logger.info("Обновляем pip...")
            subprocess.run([
                "python", "-m", "pip", "install", "--upgrade", "pip"
            ])
        else:
            logger.info(f"Обновляем пакет: {package_name}")
            subprocess.run(["pip", "install", "--upgrade", package_name])

@time_spent
def update_table():
    logger.info("Запускаю обновление таблиц")
    current_year = datetime.now().year - 1
    for content_type, content_type_url in CATEGORY_URLS.items():
        # Создаем таблицы
        db.create_table(table_name=content_type)
        # Обновляем общий топ фильмов
        get_top_movies_and_serials(content_type, content_type_url, current_year)
        # Обновляем каждый не вышедший фильм
        not_released_movies = db.get_table(table_name=content_type, data=Movie(date_now=None).to_dict, fetchone=False)
        if not_released_movies:
            check_movie_release(content_type=content_type, not_released_movies=not_released_movies)

@time_spent
def send_new_movies():
    logger.info("Запускаю отправку сообщения в telegram")

    for content_type, content_type_url in CATEGORY_URLS.items():
        # Проверяем что вышло за последнее время
        time_ago = datetime.now(timezone.utc) - timedelta(hours=2)
        recent_movies = db.get_table(
            table_name=content_type,
            data=Movie(date_now=(">=", time_ago)).to_dict,
            sort_by="rating DESC",
            fetchone=False
        )
        if recent_movies:
            for movie in recent_movies:
                movie = Movie.from_dict(movie)

                categories = movie.get_translated_categories()
                countries = movie.get_translated_countries()

                # Фильтрация
                if not Movie.filter_passes(categories, CATEGORY_BAN_LIST):
                    continue
                if not Movie.filter_passes(countries, COUNTRY_BAN_LIST):
                    continue

                year_mod = movie.year_start
                if movie.year_end:
                    year_mod = f"{movie.year_start}-{movie.year_end}"
                title_trailer = f"{movie.title_original} {movie.year_start} трейлер"
                if movie.year_end:
                    title_trailer = f"{movie.title_original} {movie.year_end} трейлер"
                title_full = f"{movie.title} ({year_mod})"

                # Кодируем название фильма для YouTube-поиска
                title_trailer_encoded = urllib.parse.quote_plus(title_trailer)
                youtube_url = f"https://www.youtube.com/results?search_query={title_trailer_encoded}"

                title_original_display = f"<b><a href='{youtube_url}'>{movie.title_original}</a></b>\n"
                if movie.title_original == movie.title:
                    title_original_display = ""

                genre_content = (
                    f"<b><u>#{content_type}</u></b>\n"
                    f"<b><a href='{youtube_url}'>{title_full}</a></b>\n"
                    f"{title_original_display}"
                    f"{countries}\n"
                    f"🎬 {categories}\n"
                    f"⭐️ <a href='{movie.url}'>{movie.rating}</a>\n"
                    f"{movie.description}")

                try:
                    # Получаем трейлер
                    youtube_links = get_youtube_links(video_name=title_trailer)
                    video_path = None
                    for youtube_link in youtube_links:
                        try:
                            video_path = download_video(url=youtube_link, output_name=title_trailer)
                            break
                        except Exception as e:
                            if "Sign in to confirm your age." in str(e):
                                continue
                            message.send_report(e)
                    if len(genre_content) <= 4096:
                        message.send_telegram_video(video_path=video_path, message=genre_content)
                    else:
                        message.send_report(f"Сообщение не отправилось. Длина сообщения: {len(genre_content)}")
                    # Удаление файла
                    if video_path:
                        os.remove(video_path)
                except Exception as e:
                    # Обнуляем дату выхода фильма
                    db.update_table(
                        table_name=content_type,
                        data=Movie(url=movie.url).to_dict,
                        updates=Movie(date_now=None).to_dict
                    )
                    message.send_report(e)

scheduler = BlockingScheduler()
scheduler.add_job(check_updates, 'cron', hour=14, minute=0)
scheduler.add_job(update_table, 'cron', hour=15, minute=0)
scheduler.add_job(send_new_movies, 'cron', hour=16, minute=0)

if __name__ == "__main__":
    scheduler.start()