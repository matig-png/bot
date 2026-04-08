FROM python:3.11-slim

WORKDIR /app

# Копируем зависимости и устанавливаем
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Копируем код бота
COPY main.py .

# Создаём директорию для данных
RUN mkdir -p /app/data

# Запуск с unbuffered выводом для логов
CMD ["python", "-u", "main.py"]
