import os
import time
from abc import ABC, abstractmethod

import psycopg2
from loguru import logger
from psycopg2.extras import DictCursor

import message


class DatabaseConnection(ABC):
    """Общий класс подключения к бд"""
    def __init__(self):
        self.connection = None

    @abstractmethod
    def create_connection(self):
        """Создает новое соединение с базой данных"""
        pass

    @abstractmethod
    def connect(self):
        """
        Устанавливает соединение с базой данных.
        В случае неудачи пытается восстановить соединение.
        """

        pass

    @abstractmethod
    def reconnect(self, retries: int = 15, delay: int = 2):
        """
        Восстанавливает соединение с базой данных.

        :param retries: число попыток переподключения
        :param delay: задержка между попытками (в секундах)
        """

        pass

    @abstractmethod
    def ensure_connection(self):
        """Проверяет активность соединения с базой данных и восстанавливает его при необходимости"""
        pass

    @abstractmethod
    def cursor(self):
        """Курсор для работы с бд"""
        pass


class DatabaseRepository:
    """Общий класс для работы с данными"""
    def __init__(self, db_connection: DatabaseConnection):
        self.db_connection = db_connection

    def table_exists(self, table_name: str) -> bool:
        """Проверяет, существует ли таблица с заданным именем"""
        pass

    def create_table(self, table_name: str):
        """Создаёт таблицу, если она ещё не существует"""
        pass

    def add_into_table(self, table_name: str, data: dict):
        """
        Добавляет новую запись в таблицу.
        :param table_name: Название таблицы.
        :param data: Словарь вида {col_name: value}.
        :return: ID вставленной записи или число затронутых строк.
        """

        pass

    def delete_from_table(self, table_name: str, data: dict):
        """
        Удаляет записи из таблицы по заданным фильтрам.
        :param table_name: Название таблицы.
        :param data: Словарь фильтров вида {col_name: value}.
        :return: Число удалённых строк.
        """

        pass

    def get_table(self, table_name: str, data: dict = None, sort_by: str = None):
        """
        Получает данные из таблицы с опциональными фильтрами и сортировкой.
        :param table_name: Название таблицы.
        :param data: Словарь фильтров вида {col_name: (operator, value)} или {col_name: value}.
        :param sort_by: Строка сортировки (например, "id ASC").
        :return: Список строк в виде словарей.
        """

        pass

    def update_table(self, table_name: str, data: dict, updates: dict):
        """
        Обновляет записи в таблице по заданным фильтрам.
        :param table_name: Название таблицы.
        :param data: Словарь фильтров вида {col_name: value}.
        :param updates: Словарь обновляемых данных вида {col_name: value}.
        :return: Число изменённых строк.
        """

        pass


class PostgreSQLConnection(DatabaseConnection):
    """Класс для работы с базой данных PostgreSQL"""

    def __init__(
        self,
        db_host: str = None,
        db_user: str = None,
        db_password: str = None,
        db_port: str = None,
        db_name: str = None
    ):
        super().__init__()
        self.db_host = db_host or os.getenv("DB_HOST")
        self.db_user = db_user or os.getenv("DB_USER")
        self.db_password = db_password or os.getenv("DB_PASSWORD")
        self.db_port = db_port or os.getenv("DB_PORT")
        self.db_name = db_name or os.getenv("DB_NAME")

        self.connect()

    def create_connection(self):
        """Создает новое соединение с базой данных"""
        return psycopg2.connect(
            host=self.db_host,
            user=self.db_user,
            password=self.db_password,
            port=self.db_port,
            dbname=self.db_name
        )

    def connect(self):
        """
        Устанавливает соединение с базой данных.
        В случае неудачи пытается восстановить соединение.
        """

        try:
            self.connection = self.create_connection()
            self.connection.autocommit = True
            logger.success(f"Успешное подключение к базе данных {self.db_name}")
        except Exception as e:
            logger.warning(f"Ошибка подключения к базе данных {self.db_name}: {e}")
            self.reconnect()

    def reconnect(self, retries: int = 15, delay: int = 2):
        """
        Восстанавливает соединение с базой данных.

        :param retries: число попыток переподключения
        :param delay: задержка между попытками (в секундах)
        """

        if self.connection:
            try:
                self.connection.close()
            except Exception as close_error:
                logger.warning(f"Ошибка при закрытии старого соединения: {close_error}")

        for attempt in range(1, retries + 1):
            try:
                self.connection = self.create_connection()
                self.connection.autocommit = True
                logger.info("Подключение к базе данных восстановлено")
                return  # Успешное подключение – выходим из метода
            except Exception as e:
                logger.warning(f"Попытка {attempt} из {retries} не удалась: {e}")
                time.sleep(delay)

        # Если все попытки не удались, отправляем отчёт об ошибке
        message.send_report(f"Не удалось восстановить соединение с базой данных после {retries} попыток.")

    def ensure_connection(self):
        """Проверяет активность соединения с базой данных и восстанавливает его при необходимости"""
        if self.connection is None or self.connection.closed:
            self.reconnect()
        else:
            try:
                with self.connection.cursor() as cursor:
                    cursor.execute("SELECT 1")
            except Exception as e:
                logger.warning(f"Соединение с базой данных потеряно: {e}")
                self.reconnect()

    def cursor(self):
        """Курсор для работы с бд"""
        self.ensure_connection()
        return self.connection.cursor(cursor_factory=DictCursor)


class PostgreSQLDatabaseRepository(DatabaseRepository):
    """Класс для работы с данными в PostgreSQL"""
    def __init__(self, db_connection: DatabaseConnection):
        super().__init__(db_connection)

    def table_exists(self, table_name: str) -> bool:
        """Проверяет, существует ли таблица с заданным именем"""
        with self.db_connection.cursor() as cursor:
            cursor.execute(
                """SELECT EXISTS (SELECT 1 FROM pg_tables WHERE tablename = %s)""", (table_name,)
            )
            result = cursor.fetchone()
            return result['exists']

    def create_table(self, table_name: str):
        """Создаёт таблицу, если она ещё не существует"""
        with self.db_connection.cursor() as cursor:
            if not self.table_exists(table_name):
                cursor.execute(f"""
                    CREATE TABLE {table_name} 
                    (
                        id SERIAL PRIMARY KEY,
                        title TEXT NOT NULL,
                        title_original TEXT,
                        year_start INT,
                        year_end INT,
                        categories TEXT[],
                        rating FLOAT,
                        description TEXT,
                        countries TEXT[],
                        url TEXT,
                        date_now TIMESTAMPTZ
                    )
                """)
                self.db_connection.connection.commit()
                logger.success(f"Таблица '{table_name}' создана")

    def add_into_table(self, table_name: str, data: dict) -> int:
        """
        Добавляет новую запись в таблицу.
        :param table_name: Название таблицы.
        :param data: Словарь вида {col_name: value}.
        :return: ID.
        """

        if not data:
            raise ValueError("Необходимо указать данные для вставки.")

        with self.db_connection.cursor() as cursor:
            # Генерация частей SQL-запроса
            columns = ", ".join(data.keys())
            placeholders = ", ".join(["%s"] * len(data))
            values = list(data.values())

            query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders}) RETURNING id"
            cursor.execute(query, values)
            self.db_connection.connection.commit()

            result = cursor.fetchone()
            return result['id'] if result and 'id' in result else None

    def delete_from_table(self, table_name: str, data: dict) -> int:
        """
        Удаляет записи из таблицы по заданным фильтрам.
        :param table_name: Название таблицы.
        :param data: Словарь фильтров вида {col_name: value}.
        :return: Число удалённых строк.
        """

        if not data:
            raise ValueError("Необходимо указать фильтры для удаления.")

        with self.db_connection.cursor() as cursor:
            # Генерация частей SQL-запроса
            where_clause = " AND ".join([f"{key} = %s" for key in data.keys()])
            params = list(data.values())

            query = f"DELETE FROM {table_name} WHERE {where_clause}"
            cursor.execute(query, params)
            self.db_connection.connection.commit()
            return cursor.rowcount

    def get_table(
        self,
        table_name: str,
        data: dict = None,
        sort_by: str = None,
        fetchone: bool = True
    ) -> dict | list[dict]:
        """
        Получает все данные из таблицы с опциональными фильтрами и сортировкой.
        :param table_name: Название таблицы.
        :param data: Словарь фильтров вида {col_name: (operator, value)} или {col_name: value}.
        :param sort_by: Строка сортировки (например, "id ASC").
        :param fetchone: Вернуть лишь одно значение, False - вернуть список
        """

        with self.db_connection.cursor() as cursor:
            where_clause = ""
            params = []
            if data:
                filter_clauses = []
                for key, condition in data.items():
                    if isinstance(condition, tuple):
                        operator, value = condition
                        filter_clauses.append(f"{key} {operator} %s")
                        params.append(value)
                    elif condition is None:  # Обрабатываем фильтры с None как IS NULL
                        filter_clauses.append(f"{key} IS NULL")
                    else:
                        filter_clauses.append(f"{key} = %s")
                        params.append(condition)
                where_clause = " WHERE " + " AND ".join(filter_clauses)

            order_by_clause = f" ORDER BY {sort_by}" if sort_by else ""

            query = f"SELECT * FROM {table_name}{where_clause}{order_by_clause}"
            cursor.execute(query, params)
            if fetchone:
                return cursor.fetchone()
            return cursor.fetchall()

    def update_table(self, table_name: str, data: dict, updates: dict) -> int:
        """
        Обновляет записи в таблице по заданным фильтрам.
        :param table_name: Название таблицы.
        :param data: Словарь фильтров вида {col_name: value}.
        :param updates: Словарь обновляемых данных вида {col_name: value}.
        :return: Число изменённых строк.
        """

        if not data or not updates:
            raise ValueError("Необходимо указать data и updates.")

        with self.db_connection.cursor() as cursor:
            # Генерация частей SQL-запроса
            set_clause = ", ".join([f"{key} = %s" for key in updates.keys()])
            where_clause = " AND ".join([f"{key} = %s" for key in data.keys()])

            query = f"UPDATE {table_name} SET {set_clause} WHERE {where_clause}"
            params = list(updates.values()) + list(data.values())

            cursor.execute(query, params)
            self.db_connection.connection.commit()
            return cursor.rowcount


db_connection = PostgreSQLConnection()
db = PostgreSQLDatabaseRepository(db_connection)

if __name__ == "__main__":
    pass