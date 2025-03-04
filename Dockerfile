# Укажите базовый образ Python
FROM python:3.12

# Установите рабочую директорию
WORKDIR /app

# Скопируйте все файлы в контейнер
COPY . /app

# Установите зависимости из файла requirements.txt, если он есть
RUN pip install --no-cache-dir -r requirements.txt || echo "No requirements.txt found"

# Запуск main.py
CMD ["python", "main.py"]
