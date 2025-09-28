from __future__ import annotations

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from datetime import datetime
import requests
import pandas as pd

# Define as configurações do BigQuery aqui para facilitar o acesso
GCP_PROJECT = "enap-aula1-cd"
BQ_DATASET = "openfda_events"
BQ_TABLE = "acetaminophen_daily"
GCP_CONN_ID = "google_cloud_default"


def generate_query_url(logical_date) -> str:
    """Gera a URL da API para buscar o total de eventos em um dia específico."""
    date_str = logical_date.strftime('%Y%m%d')
    return (
        "https://api.fda.gov/drug/event.json"
        f"?search=patient.drug.activesubstance.name:%22acetaminophen%22"
        f"+AND+receivedate:[{date_str}+TO+{date_str}]"
    )


@task
def fetch_openfda_data() -> dict | None:
    """
    Busca o total de eventos de Acetaminophen do OpenFDA para o dia da execução da DAG.
    Retorna um dicionário com a data e a contagem, que será passado via XCom.
    """
    ctx = get_current_context()
    logical_date = ctx["data_interval_start"]
    url = generate_query_url(logical_date)
    
    print(f"Fetching data from URL: {url}")
    
    try:
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
        # A API com ?search= (sem &count=) retorna o total no campo 'total' dos metadados
        total_events = resp.json().get("meta", {}).get("results", {}).get("total", 0)
    except requests.RequestException as e:
        print(f"OpenFDA request failed: {e}")
        return None

    if total_events == 0:
        print(f"No events found for {logical_date.strftime('%Y-%m-%d')}.")
        return None
    
    return {
        "report_date": logical_date.strftime('%Y-%m-%d'),
        "event_count": total_events
    }


@task
def save_to_bigquery(data: dict | None) -> None:
    """
    Salva um único registro (dicionário) em uma tabela do BigQuery,
    reutilizando a lógica do projeto anterior.
    """
    if not data:
        print("No data to write to BigQuery.")
        return

    from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
    
    # Converte o dicionário em um DataFrame de uma única linha
    df = pd.DataFrame([data])
    # Garante que a coluna de data seja do tipo datetime para o to_gbq
    df['report_date'] = pd.to_datetime(df['report_date'])

    bq_hook = BigQueryHook(gcp_conn_id=GCP_CONN_ID, use_legacy_sql=False)
    credentials = bq_hook.get_credentials()
    
    table_schema = [
        {'name': 'report_date', 'type': 'DATE'},
        {'name': 'event_count', 'type': 'INTEGER'},
    ]

    df.to_gbq(
        destination_table=f"{BQ_DATASET}.{BQ_TABLE}",
        project_id=GCP_PROJECT,
        if_exists="append",
        credentials=credentials,
        table_schema=table_schema,
        progress_bar=False,
    )
    print(f"Loaded 1 row to {GCP_PROJECT}.{BQ_DATASET}.{BQ_TABLE}")


# Argumentos padrão para a DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
}


@dag(
    dag_id="openfda_acetaminophen_to_bq_daily",
    description="ETL diário de eventos do OpenFDA para Acetaminophen para o BigQuery",
    default_args=default_args,
    schedule="@daily",
    start_date=datetime(2025, 9, 1),
    catchup=False, # Comece com False para facilitar os testes
    tags=["openfda", "bigquery", "tylenol"],
)
def openfda_etl_dag():
    event_data = fetch_openfda_data()
    save_to_bigquery(event_data)

# Instancia a DAG para que o Airflow a reconheça
dag = openfda_etl_dag()
