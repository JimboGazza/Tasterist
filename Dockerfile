FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENV PORT=8501
EXPOSE 8501

CMD ["gunicorn", "--workers", "2", "--threads", "4", "--timeout", "120", "--bind", "0.0.0.0:8501", "wsgi:app"]

