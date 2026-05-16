FROM python:3.13-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY ai_scorer_worker.py .

CMD ["python", "ai_scorer_worker.py"]

