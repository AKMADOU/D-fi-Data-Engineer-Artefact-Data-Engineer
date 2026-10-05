#!/bin/sh
# Initialisation MinIO (idempotente) : buckets + utilisateur de service.
#   raw-landing : fichiers bruts (<pays>/<dataset>/...)
#   lakehouse   : données et métadonnées Iceberg
#   archive     : fichiers traités, déplacés après ingestion réussie
#   regulatory-reports : déclarations BCEAO / CIMA (Level 2)
set -eu

# MINIO_URL : http://minio:9000 (docker compose) ou le Service Kubernetes (Level 4)
for i in $(seq 1 30); do mc alias set local "${MINIO_URL:-http://minio:9000}" "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" >/dev/null 2>&1 && break; echo "attente de MinIO ($i)"; sleep 5; done
mc alias set local "${MINIO_URL:-http://minio:9000}" "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" >/dev/null

for bucket in raw-landing lakehouse archive regulatory-reports; do
  mc mb --ignore-existing "local/$bucket"
done
# Versioning : aucune perte possible d'un fichier brut ni d'une déclaration réglementaire
mc version enable local/archive
mc version enable local/regulatory-reports

# Les applications n'utilisent pas le compte root : compte de service limité aux 3 buckets.
cat > /tmp/lakehouse-policy.json <<'EOF'
{
  "Version": "2012-10-17",
  "Statement": [
    {"Effect": "Allow",
     "Action": ["s3:GetBucketLocation", "s3:ListBucket", "s3:ListBucketMultipartUploads"],
     "Resource": ["arn:aws:s3:::raw-landing", "arn:aws:s3:::lakehouse", "arn:aws:s3:::archive",
                  "arn:aws:s3:::regulatory-reports"]},
    {"Effect": "Allow",
     "Action": ["s3:GetObject", "s3:PutObject", "s3:DeleteObject",
                "s3:AbortMultipartUpload", "s3:ListMultipartUploadParts"],
     "Resource": ["arn:aws:s3:::raw-landing/*", "arn:aws:s3:::lakehouse/*",
                  "arn:aws:s3:::archive/*", "arn:aws:s3:::regulatory-reports/*"]}
  ]
}
EOF
# Policy versionnée : une nouvelle version s'ajoute sans casser un déploiement existant.
POLICY=lakehouse-rw-v2
mc admin policy create local "$POLICY" /tmp/lakehouse-policy.json
mc admin user add local "$LAKEHOUSE_S3_ACCESS_KEY" "$LAKEHOUSE_S3_SECRET_KEY"
mc admin policy attach local "$POLICY" --user "$LAKEHOUSE_S3_ACCESS_KEY" 2>/dev/null \
  || echo "policy $POLICY déjà attachée"

echo "MinIO initialisé : $(mc ls local | wc -l) buckets"
