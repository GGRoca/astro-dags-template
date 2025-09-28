from __future__ import annotations

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from datetime import timedelta
import pendulum
import requests
import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook


DEFAULT_ARGS = {
    "email_on_failure": True,
    "owner": "Alex Lopes,Open in Cloud IDE",
}


@task
def fetch_bitcoin_history_from_coingecko():
    """
    Coleta dados horários do Bitcoin dos últimos 2 meses e substitui os dados
    na tabela Postgres.
    """
    ctx = get_current_context()

    # --- LÓGICA TEMPORÁRIA: Mudar a janela de tempo para 2 meses ---
    end_time = pendulum.now("UTC")
    start_time = end_time.subtract(months=2)

    print(f"[UTC] janela-alvo: {start_time} -> {end_time}")

    start_s = int(start_time.timestamp())
    end_s = int(end_time.timestamp())

    url = "https://api.coingecko.com/api/v3/coins/bitcoin/market_chart/range"
    params = {
        "vs_currency": "usd",
        "from": start_s,
        "to": end_s,
    }

    r = requests.get(url, params=params, timeout=60) # Timeout aumentado
    r.raise_for_status()
    payload = r.json()
    
    prices = payload.get("prices", [])
    if not prices:
        print("Sem dados retornados pela API para a janela especificada.")
        return

    df_p = pd.DataFrame(prices, columns=["time_ms", "price_usd"])
    df_c = pd.DataFrame(payload.get("market_caps", []), columns=["time_ms", "market_cap_usd"])
    df_v = pd.DataFrame(payload.get("total_volumes", []), columns=["time_ms", "volume_usd"])
    df = df_p.merge(df_c, on="time_ms", how="outer").merge(df_v, on="time_ms", how="outer")
    df["time"] = pd.to_datetime(df["time_ms"], unit="ms", utc=True)
    df.drop(columns=["time_ms"], inplace=True)
    df.set_index("time", inplace=True)
    df.sort_index(inplace=True)

    print(df.head(10).to_string())

    hook = PostgresHook(postgres_conn_id="postgres")

    # --- LÓGICA TEMPORÁRIA: Limpar a tabela antes de inserir ---
    table_name = "bitcoin_history_guilherme"
    print(f"Limpando dados antigos da tabela '{table_name}'...")
    hook.run(f"DELETE FROM {table_name};")
    print("Tabela limpa. Carregando novos dados...")

    engine = hook.get_sqlalchemy_engine()
    df.to_sql(table_name, con=engine, if_exists="append", index=True)
    print(f"Carregados {len(df)} registros na tabela '{table_name}'.")


@dag(
    default_args=DEFAULT_ARGS,
    schedule="0 0 * * *",  # diário à 00:00 UTC
    start_date=pendulum.datetime(2025, 9, 17, tz="UTC"),
    catchup=True,
    owner_links={
        "Alex Lopes": "mailto:alexlopespereira@gmail.com",
        "Open in Cloud IDE": "https://cloud.astronomer.io/...",
    },
    tags=["bitcoin", "etl", "coingecko"],
)
def bitcoin_etl_coingecko():
    fetch_bitcoin_history_from_coingecko()

dag = bitcoin_etl_coingecko()
