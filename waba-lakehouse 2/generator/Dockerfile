FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir --default-timeout=300 --retries 10 -r requirements.txt

COPY waba_gen ./waba_gen
COPY app.py .
COPY tests ./tests

# Utilisateur non-root ; /data contient l'état des référentiels (volume)
RUN useradd --create-home --uid 1000 app && mkdir -p /data/state && chown -R app /data
USER app

EXPOSE 8501
HEALTHCHECK --interval=30s --timeout=5s --retries=3 \
  CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8501/_stcore/health')"
CMD ["streamlit", "run", "app.py", "--server.port=8501", "--server.address=0.0.0.0", \
     "--browser.gatherUsageStats=false"]
