import datetime

from database import db
from movie import Movie

not_released_movies = db.get_table(table_name="Фильмы", data=Movie(date_now=None).to_dict)
for movie in not_released_movies:
    print(Movie.from_dict(movie))

