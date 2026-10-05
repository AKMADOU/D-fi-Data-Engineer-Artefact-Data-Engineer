"""Dépose dans raw-landing un fichier bank_transactions volontairement défectueux
(montant non numérique, devise incohérente, compte inconnu, ligne tronquée) pour
démontrer la Dead Letter Queue. Exécuté dans le conteneur generator (boto3 + env S3)."""
import os
from datetime import datetime, timezone
import uuid

import boto3

now = datetime.now(timezone.utc)
ts = now.strftime("%Y-%m-%dT%H:%M:%SZ")
header = ("transaction_id,timestamp,account_id,beneficiary_account,branch_id,country_code,"
          "transaction_type,amount,currency,channel,transaction_status,fee_amount,entity_type")
rows = [
    f"{uuid.uuid4()},{ts},WABA-CI-A-0000001,,WABA-CI-B-001,CI,WITHDRAWAL,abc,XOF,ATM,SUCCESS,0,BANK",
    f"{uuid.uuid4()},{ts},WABA-CI-A-0000001,,WABA-CI-B-001,CI,WITHDRAWAL,1000,GHS,ATM,SUCCESS,0,BANK",
    f"{uuid.uuid4()},{ts},WABA-CI-A-9999999,,WABA-CI-B-001,CI,WITHDRAWAL,1000,XOF,ATM,SUCCESS,0,BANK",
    f"{uuid.uuid4()},{ts},WABA-CI-A-0000001",
]
key = f"CI/bank_transactions/bank_txn_CI_{now:%Y%m%d}_9{now:%H%M%S}.csv"
s3 = boto3.client("s3", endpoint_url=os.environ["S3_ENDPOINT"],
                  aws_access_key_id=os.environ["S3_ACCESS_KEY"],
                  aws_secret_access_key=os.environ["S3_SECRET_KEY"],
                  region_name=os.environ.get("AWS_REGION", "us-east-1"))
s3.put_object(Bucket=os.environ.get("LANDING_BUCKET", "raw-landing"), Key=key,
              Body=("\n".join([header, *rows]) + "\n").encode())
print(f'{{"status":"SUCCESS","file":"{key}","bad_rows":{len(rows)}}}')
