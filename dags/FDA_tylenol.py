from __future__ import annotations

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from datetime import datetime, timedelta
import requests
import pandas as pd

# ----------------------- Configurações -----------------------
GCP_PROJECT = "enap-aula1-cd"
BQ_DATASET = "FDA_API"                 # << mudou para o novo dataset
BQ_TABLE = "acetaminophen_daily"       # tabela final (sem partição)
GCP_CONN_ID = "google_cloud_default"

# Backfill inicial
HIST_START = datetime(2024, 1, 1).date()

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
}

# ----------------------- Helpers -----------------------
def build_url(start_date: datetime, end_date: datetime) -> str:
    """
    Série diária no intervalo [start_date, end_date] (inclusive).
    Usa medicinalproduct:acetaminophen (sem aspas) e count=receivedate (limit<=1000).
    """
    s = start_date.strftime("%Y%m%d")
    e = end_date.strftime("%Y%m%d")
    return (
        "https://api.fda.gov/drug/event.json"
        "?search=patient.drug.medicinalproduct:acetaminophen"
        f"+AND+receivedate:[{s}+TO+{e}]"
        "&count=receivedate"
        "&limit=1000"
    )

# ----------------------- Tasks -----------------------
@task
def ensure_table() -> None:
    """Cria a tabela (SEM partição) se ainda não existir."""
    from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
    bq = BigQueryHook(gcp_conn_id=GCP_CONN_ID, use_legacy_sql=False)
    client = bq.get_client(project_id=GCP_PROJECT)
    ddl = f"""
    CREATE TABLE IF NOT EXISTS `{GCP_PROJECT}.{BQ_DATASET}.{BQ_TABLE}` (
      report_date DATE,
      event_count INT64
    )
    """
    client.query(ddl).result()

@task
def compute_range() -> dict:
    """
    Lê MAX(report_date). Se vazio, começa em 2024-01-01.
    Fecha em (data_interval_end - 1 dia) para evitar dia parcial.
    """
    from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

    ctx = get_current_context()
    end_date = (ctx["data_interval_end"] - timedelta(days=1)).date()

    bq = BigQueryHook(gcp_conn_id=GCP_CONN_ID, use_legacy_sql=False)
    client = bq.get_client(project_id=GCP_PROJECT)
    try:
        rows = list(client.query(
            f"SELECT MAX(report_date) mx FROM `{GCP_PROJECT}.{BQ_DATASET}.{BQ_TABLE}`"
        ).result())
        mx = rows[0]["mx"] if rows and rows[0]["mx"] is not None else None
    except Exception:
        mx = None

    start_date = HIST_START if mx is None else (mx + timedelta(days=1))
    if start_date > end_date:
        return {"start": None, "end": None}
    return {"start": start_date.isoformat(), "end": end_date.isoformat()}

@task
def fetch_openfda_data(dr: dict) -> list[dict]:
    """
    Busca a série diária e retorna [{"report_date": "YYYY-MM-DD", "event_count": N}, ...].
    """
    if not dr or not dr["start"] or not dr["end"]:
        return []

    start_date = datetime.fromisoformat(dr["start"])
    end_date = datetime.fromisoformat(dr["end"])
    url = build_url(start_date, end_date)
    print(f"[openFDA] GET {url}")

    try:
        resp = requests.get(url, timeout=60)
        resp.raise_for_status()
        payload = resp.json()
    except requests.RequestException as e:
        print(f"Falha openFDA: {e}")
        return []

    rows = []
    for r in payload.get("results", []):
        # r = {"time": "YYYYMMDD", "count": N}
        d = pd.to_datetime(r["time"], format="%Y%m%d").date().isoformat()
        rows.append({"report_date": d, "event_count": int(r["count"])})
    rows.sort(key=lambda x: x["report_date"])
    print(f"[openFDA] Linhas: {len(rows)}")
    return rows

@task
def save_to_bigquery(rows: list[dict]) -> None:
    """Append simples na tabela final (sem partição)."""
    if not rows:
        print("Nada novo para gravar.")
        return
    from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
    df = pd.DataFrame(rows)
    df["report_date"] = pd.to_datetime(df["report_date"])

    bq = BigQueryHook(gcp_conn_id=GCP_CONN_ID, use_legacy_sql=False)
    credentials = bq.get_credentials()

    df.to_gbq(
        destination_table=f"{BQ_DATASET}.{BQ_TABLE}",
        project_id=GCP_PROJECT,
        if_exists="append",
        credentials=credentials,
        progress_bar=False,
    )
    print(f"[BigQuery] Gravadas {len(df)} linhas em {GCP_PROJECT}.{BQ_DATASET}.{BQ_TABLE}")

# ----------------------- DAG -----------------------
@dag(
    dag_id="openfda_acetaminophen_to_bq_daily",  # mantém o ID original
    description="ETL mensal do OpenFDA (acetaminophen) → BigQuery, com backfill incremental.",
    default_args=default_args,
    schedule="0 6 10 * *",   # roda no dia 10 de cada mês (06:00 UTC)
    start_date=datetime(2024, 1, 1),
    catchup=False,           # backfill acontece na 1ª execução via compute_range()
    tags=["openfda", "bigquery", "tylenol"],
)
def openfda_etl_dag():
    ensure = ensure_table()
    dr = compute_range()
    rows = fetch_openfda_data(dr)
    write = save_to_bigquery(rows)
    ensure >> dr >> rows >> write

dag = openfda_etl_dag()
