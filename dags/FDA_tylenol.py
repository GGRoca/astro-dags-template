from __future__ import annotations

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from datetime import datetime, timedelta
import requests
import pandas as pd

# ----------------------- Configurações -----------------------
GCP_PROJECT = "enap-aula1-cd"
BQ_DATASET = "openfda_events"
BQ_TABLE = "acetaminophen_daily"            # tabela final (deduplicada por report_date)
BQ_STAGE_TABLE = "acetaminophen_daily_stage"  # staging para MERGE
GCP_CONN_ID = "google_cloud_default"

# Janela histórica inicial do backfill
HIST_START = datetime(2024, 1, 1).date()

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
}


# ----------------------- Helpers -----------------------
def generate_query_url(start_date: datetime, end_date: datetime) -> str:
    """
    Retorna a URL da API do openFDA para série diária (count=receivedate)
    no intervalo [start_date, end_date], ambas YYYYMMDD.
    """
    start_str = start_date.strftime("%Y%m%d")
    end_str = end_date.strftime("%Y%m%d")
    return (
        "https://api.fda.gov/drug/event.json"
        "?search=patient.drug.openfda.substance_name:%22ACETAMINOPHEN%22"
        f"+AND+receivedate:[{start_str}+TO+{end_str}]"
        "&count=receivedate"
        "&limit=10000"
    )


# ----------------------- Tasks -----------------------
@task
def ensure_tables() -> None:
    """
    Garante que a tabela final exista (schema simples) e cria a stage se necessário.
    Não recria nada se já existir.
    """
    from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

    bq = BigQueryHook(gcp_conn_id=GCP_CONN_ID, use_legacy_sql=False)
    client = bq.get_client(project_id=GCP_PROJECT)

    # Cria tabela final, se não existir (particionada por report_date opcionalmente)
    ddl_final = f"""
    CREATE TABLE IF NOT EXISTS `{GCP_PROJECT}.{BQ_DATASET}.{BQ_TABLE}` (
      report_date DATE,
      event_count INT64
    )
    PARTITION BY DATE(report_date)
    """
    client.query(ddl_final).result()

    # Cria tabela de stage, se não existir
    ddl_stage = f"""
    CREATE TABLE IF NOT EXISTS `{GCP_PROJECT}.{BQ_DATASET}.{BQ_STAGE_TABLE}` (
      report_date DATE,
      event_count INT64
    )
    """
    client.query(ddl_stage).result()


@task
def get_date_range() -> dict:
    """
    Lê do BigQuery o MAX(report_date) e define o intervalo incremental:
    - start = max(report_date)+1 dia, ou HIST_START se tabela vazia
    - end   = data_interval_end - 1 dia (evita dia parcial)
    """
    from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

    ctx = get_current_context()
    end_date = (ctx["data_interval_end"] - timedelta(days=1)).date()

    bq = BigQueryHook(gcp_conn_id=GCP_CONN_ID, use_legacy_sql=False)
    client = bq.get_client(project_id=GCP_PROJECT)

    sql_max = f"SELECT MAX(report_date) AS mx FROM `{GCP_PROJECT}.{BQ_DATASET}.{BQ_TABLE}`"
    try:
        rows = list(client.query(sql_max).result())
        mx = rows[0]["mx"] if rows and rows[0]["mx"] is not None else None
    except Exception:
        # Se dataset/table ainda não existem, começamos do HIST_START
        mx = None

    if mx is None:
        start_date = HIST_START
    else:
        start_date = (mx + timedelta(days=1))

    # Se o start ultrapassar o end, não há nada novo
    if start_date > end_date:
        return {"start": None, "end": None}

    return {"start": start_date.isoformat(), "end": end_date.isoformat()}


@task
def fetch_openfda_data(date_range: dict) -> list[dict]:
    """
    Busca a série diária no openFDA entre date_range['start'] e date_range['end'] (incluindo ambas).
    Retorna lista de dicts [{"report_date": "YYYY-MM-DD", "event_count": N}, ...].
    """
    if not date_range or not date_range["start"] or not date_range["end"]:
        return []

    start_date = datetime.fromisoformat(date_range["start"])
    end_date = datetime.fromisoformat(date_range["end"])

    url = generate_query_url(start_date, end_date)
    print(f"[openFDA] GET {url}")

    try:
        resp = requests.get(url, timeout=60)
        resp.raise_for_status()
        payload = resp.json()
    except requests.RequestException as e:
        print(f"Falha na chamada openFDA: {e}")
        return []

    rows = []
    for r in payload.get("results", []):
        # r: {"time": "YYYYMMDD", "count": N}
        d = pd.to_datetime(r["time"], format="%Y%m%d").date().isoformat()
        rows.append({"report_date": d, "event_count": int(r["count"])})

    # Ordena por data para consistência
    rows.sort(key=lambda x: x["report_date"])
    print(f"[openFDA] Linhas recebidas: {len(rows)}")
    return rows


@task
def upsert_to_bigquery(rows: list[dict]) -> None:
    """
    Carrega 'rows' para tabela de stage e faz MERGE na tabela final por report_date.
    Assim, podemos rodar agora (backfill) e, depois, mensalmente sem duplicar.
    """
    if not rows:
        print("Sem linhas novas para carregar.")
        return

    from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

    # Carrega para a tabela de stage (substitui o conteúdo da stage neste run)
    df = pd.DataFrame(rows)
    df["report_date"] = pd.to_datetime(df["report_date"])

    bq_hook = BigQueryHook(gcp_conn_id=GCP_CONN_ID, use_legacy_sql=False)
    credentials = bq_hook.get_credentials()

    # Escreve na stage com replace (apenas desta execução)
    df.to_gbq(
        destination_table=f"{BQ_DATASET}.{BQ_STAGE_TABLE}",
        project_id=GCP_PROJECT,
        if_exists="replace",
        credentials=credentials,
        table_schema=[
            {"name": "report_date", "type": "DATE"},
            {"name": "event_count", "type": "INTEGER"},
        ],
        progress_bar=False,
    )
    print(f"[BigQuery] Stage carregada: {len(df)} linhas.")

    # MERGE incremental por report_date (upsert)
    client = bq_hook.get_client(project_id=GCP_PROJECT)
    merge_sql = f"""
    MERGE `{GCP_PROJECT}.{BQ_DATASET}.{BQ_TABLE}` T
    USING `{GCP_PROJECT}.{BQ_DATASET}.{BQ_STAGE_TABLE}` S
    ON T.report_date = S.report_date
    WHEN MATCHED THEN
      UPDATE SET event_count = S.event_count
    WHEN NOT MATCHED THEN
      INSERT (report_date, event_count) VALUES (S.report_date, S.event_count)
    """
    client.query(merge_sql).result()
    print("[BigQuery] MERGE concluído com sucesso.")


# ----------------------- DAG -----------------------
@dag(
    dag_id="openfda_acetaminophen_to_bq_monthly_incremental",
    description="Backfill desde 2024 e atualização mensal (dia 10) de eventos do openFDA (Acetaminophen) no BigQuery",
    default_args=default_args,
    schedule="0 6 10 * *",  # roda dia 10 de cada mês (06:00 UTC)
    start_date=datetime(2024, 1, 1),
    catchup=False,  # execução mensal daqui pra frente; backfill é coberto na primeira execução via lógica incremental
    tags=["openfda", "bigquery", "tylenol"],
)
def openfda_etl_dag():
    ensure = ensure_tables()
    drange = get_date_range()
    rows = fetch_openfda_data(drange)
    upsert = upsert_to_bigquery(rows)

    ensure >> drange >> rows >> upsert


dag = openfda_etl_dag()
