# Image d'initialisation MinIO : client mc (binaire Go statique) + un shell pour init.sh.
# Les images officielles minio/minio et minio/mc ne sont plus téléchargeables (Docker Hub
# et quay.io, sept. 2026) : on utilise le build communautaire pgsty/mc.
FROM pgsty/mc:RELEASE.2026-09-16T00-00-00Z AS mc
FROM python:3.11-slim
COPY --from=mc /usr/bin/mc /usr/local/bin/mc
COPY init.sh /init/init.sh
ENTRYPOINT ["/bin/sh", "/init/init.sh"]
