from dataclasses import dataclass
from datetime import datetime
from typing import Optional, Union

from dictionaries import (CATEGORY_TRANSLATED, COUNTRY_TRANSLATED,
                          rename_from_dict)


@dataclass
class Movie:
    id: Optional[Union[int, tuple[str, int]]] = ...
    title: Optional[str] = ...
    title_original: Optional[str] = ...
    year_start: Optional[Union[int, tuple[str, int]]] = ...
    year_end: Optional[Union[int, tuple[str, int]]] = ...
    categories: Optional[list[str]] = ...
    rating: Optional[Union[float, tuple[str, float]]] = ...
    description: Optional[str] = ...
    countries: Optional[list[str]] = ...
    url: Optional[str] = ...
    date_now: Optional[Union[datetime, tuple[str, datetime]]] = ...

    @property
    def to_dict(self):
        return {k: v for k, v in self.__dict__.items() if v is not ...}

    def get_translated_categories(self) -> str:
        categories = rename_from_dict(
            items=self.categories,
            item_translates=CATEGORY_TRANSLATED,
            prefix="#"
        )
        return categories

    def get_translated_countries(self) -> str:
        countries = rename_from_dict(
            items=self.countries,
            item_translates=COUNTRY_TRANSLATED,
        )
        return countries

    @staticmethod
    def filter_passes(types: list[str], ban_list: list[str]) -> bool:
        return not any(banned in types for banned in ban_list)

    @classmethod
    def from_dict(cls, data: dict):
        return cls(**data) if data else None

