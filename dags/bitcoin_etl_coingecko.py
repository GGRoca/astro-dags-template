from __future__ import annotations

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from datetime import timedelta
import pendulum
import requests
import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook

# --- DOCUMENTAÇÃO DO PROCESSO ---
# NOTA HISTÓRICA: A carga inicial desta tabela foi realizada em 28/09/2025.
# Foi utilizada uma versão temporária desta DAG para primeiro limpar a tabela com o comando DELETE
# e para realizar um backfill de 2 meses de dados históricos da API CoinGecko.
# Fiz isso pois a DAG iniical do projeto só pegava 2 dias de dados.
#
# Esta versão do código é a projetada para rodar diariamente
# e anexar (append) apenas os dados do dia anterior, mantendo o histórico.
# O parâmetro 'catchup' da DAG está definido como False para evitar re-execuções
# de períodos históricos já populados.


DEFAULT_ARGS = {
    "email_on_failure": True,
    "owner": "Alex Lopes,Open in Cloud IDE",
}


@task
def fetch_bitcoin_history_from_coingecko():
    """
    Coleta dados horários do Bitcoin na janela do dia anterior ("yesterday")
    e anexa na tabela Postgres.
    """
    ctx = get_current_context()

    # <-- LÓGICA INCREMENTAL DIÁRIA: Pega a janela de execução do Airflow.
    end_time = ctx["data_interval_start"]
    start_time = end_time - timedelta(days=1)

    print(f"[UTC] janela-alvo: {start_time} -> {end_time}")

    start_s = int(start_time.timestamp())
    end_s = int(end_time.timestamp())

    url = "https://api.coingecko.com/api/v3/coins/bitcoin/market_chart/range"
    params = {
        "vs_currency": "usd",
        "from": start_s,
        "to": end_s,
    }
    
    r = requests.get(url, params=params, timeout=30)
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
    engine = hook.get_sqlalchemy_engine()
    
    # <-- LÓGICA DE CARGA: Apenas anexa os novos dados.
    df.to_sql("bitcoin_history_guilherme", con=engine, if_exists="append", index=True)
    # Criei uma tabela de nom "_Guilherme", com maiúsucula, e ela não funcionou. Ao criar com minúscula o Looker Studio leu a tabela normalmente.
    print(f"Carregados {len(df)} novos registros.")


@dag(
    default_args=DEFAULT_ARGS,
    schedule="0 0 * * *", # Roda todo dia à meia-noite UTC (21:00h no seu fuso)
    start_date=pendulum.datetime(2025, 9, 17, tz="UTC"),
    catchup=False, # <-- MUITO IMPORTANTE: Evita re-execuções de dados históricos.
    owner_links={
        "Alex Lopes": "mailto:alexlopespereira@gmail.com",
        "Open in Cloud IDE": "https://cloud.astronomer.io/...",
    },
    tags=["bitcoin", "etl", "coingecko"],
)
def bitcoin_etl_coingecko():
    fetch_bitcoin_history_from_coingecko()

dag = bitcoin_etl_coingecko()
