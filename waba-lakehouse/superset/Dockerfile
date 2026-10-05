# Superset 6.1 + pilote Trino + Authlib (SSO OpenID Connect Keycloak) — Level 4
# L'image officielle est « lean » : aucun pilote de base de données analytique.
FROM apache/superset:6.1.0

USER root
RUN . /app/.venv/bin/activate && \
    (uv pip install --no-cache "trino[sqlalchemy]==0.340.0" "authlib==1.8.0" psycopg2-binary \
     || python -m pip install --no-cache-dir "trino[sqlalchemy]==0.340.0" "authlib==1.8.0" psycopg2-binary)

# Bundle des 3 dashboards (généré en Python, importé par le Job superset-init)
COPY bootstrap /app/bootstrap
COPY init.sh /app/bootstrap/init.sh
RUN chmod 755 /app/bootstrap/init.sh
USER superset
