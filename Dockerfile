FROM python:3.11-slim
WORKDIR /app
COPY requirements-deploy.txt .
RUN pip install --no-cache-dir -r requirements-deploy.txt
COPY . .
EXPOSE 8000
CMD ["uvicorn", "demo_app:app", "--host", "0.0.0.0", "--port", "8000"]
