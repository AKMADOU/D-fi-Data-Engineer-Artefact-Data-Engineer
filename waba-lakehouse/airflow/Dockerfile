# Airflow 3.3 + providers Docker (lancement des jobs Spark) et Amazon (S3Hook -> MinIO)
FROM apache/airflow:3.3.2

COPY requirements.txt /tmp/requirements.txt
# Airflow épinglé : un provider ne peut pas le mettre à jour par effet de bord
RUN pip install --no-cache-dir --default-timeout=300 --retries 10 \
      "apache-airflow==3.3.2" -r /tmp/requirements.txt

COPY --chown=airflow:root init.sh /opt/airflow/init.sh
COPY --chown=airflow:root tests /opt/airflow/tests
# Level 4 (Kubernetes) : DAGs intégrés à l'image (docker compose les monte en volume)
COPY --chown=airflow:root dags /opt/airflow/dags
# Répertoire du fichier de mots de passe (volume partagé, propriétaire airflow)
RUN mkdir -p /opt/airflow/auth
