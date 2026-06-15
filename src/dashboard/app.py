"""Dashboard Streamlit da camada Gold."""

import os

import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import boto3
import json
from io import BytesIO

st.set_page_config(page_title="Crypto Pipeline  Dashboard", layout="wide")
st.title("Crypto Pipeline  Tendências de Criptoativos")

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_USER = os.getenv("MINIO_ROOT_USER", "minioadmin")
MINIO_PASS = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin")


@st.cache_resource
def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_USER,
        aws_secret_access_key=MINIO_PASS,
    )


@st.cache_data(ttl=60)
def load_gold_data(table: str):
    """Lê Parquet do Gold bucket via S3."""
    import pyarrow.parquet as pq

    s3 = get_s3_client()
    prefix = f"warehouse/crypto/{table}/data/"

    try:
        response = s3.list_objects_v2(Bucket="gold", Prefix=prefix)
        if "Contents" not in response:
            return None

        parquet_files = [
            obj["Key"] for obj in response["Contents"]
            if obj["Key"].endswith(".parquet")
        ]

        if not parquet_files:
            return None

        dfs = []
        for key in parquet_files:
            obj = s3.get_object(Bucket="gold", Key=key)
            table_data = pq.read_table(BytesIO(obj["Body"].read()))
            dfs.append(table_data.to_pandas())

        import pandas as pd
        return pd.concat(dfs, ignore_index=True)
    except Exception as e:
        st.error(f"Erro ao ler {table}: {e}")
        return None


# --- Sidebar ---
st.sidebar.header("Filtros")
top_n = st.sidebar.slider("Top N moedas", 10, 100, 20)

# --- Carregar dados ---
df_dim = load_gold_data("dim_moedas")
df_fct = load_gold_data("fct_metricas_hora")

if df_dim is not None and not df_dim.empty:
    df_top = df_dim.nsmallest(top_n, "market_cap_rank")

    # KPIs
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Moedas rastreadas", len(df_dim))
    col2.metric("Market Cap Total", f"${df_dim['market_cap'].sum():,.0f}")
    col3.metric("Volume 24h Total", f"${df_dim['total_volume'].sum():,.0f}")
    col4.metric("Maior variação 24h",
                f"{df_dim['price_change_percentage_24h'].max():+.2f}%")

    st.markdown("---")

    # Top moedas por Market Cap
    st.subheader(f"Top {top_n}  Market Cap")
    fig_mcap = px.bar(
        df_top.sort_values("market_cap", ascending=True),
        x="market_cap", y="name",
        orientation="h",
        color="price_change_percentage_24h",
        color_continuous_scale="RdYlGn",
        labels={"market_cap": "Market Cap (USD)", "name": ""},
    )
    fig_mcap.update_layout(height=max(400, top_n * 25))
    st.plotly_chart(fig_mcap, use_container_width=True)

    # Variação 24h
    st.subheader("Variação de Preço  24h")
    fig_var = px.bar(
        df_top.sort_values("price_change_percentage_24h"),
        x="price_change_percentage_24h", y="symbol",
        orientation="h",
        color="price_change_percentage_24h",
        color_continuous_scale="RdYlGn",
        labels={"price_change_percentage_24h": "Variação (%)", "symbol": ""},
    )
    st.plotly_chart(fig_var, use_container_width=True)

    # Spread (High - Low)
    st.subheader("Spread 24h (High - Low)")
    df_top["spread_24h"] = df_top["high_24h"] - df_top["low_24h"]
    fig_spread = px.bar(
        df_top.sort_values("spread_24h", ascending=False).head(20),
        x="symbol", y="spread_24h",
        color="spread_24h",
        color_continuous_scale="Viridis",
        labels={"spread_24h": "Spread (USD)", "symbol": ""},
    )
    st.plotly_chart(fig_spread, use_container_width=True)

else:
    st.warning(
        "Sem dados na camada Gold. "
        "Verifique se o pipeline Bronze → Silver → Gold já executou."
    )

# --- Métricas horárias (se disponível) ---
if df_fct is not None and not df_fct.empty:
    st.markdown("---")
    st.subheader("Métricas Horárias  Tendências")

    moedas = sorted(df_fct["symbol"].unique())
    selected = st.selectbox("Selecione a moeda", moedas, index=0)

    df_moeda = df_fct[df_fct["symbol"] == selected].sort_values(["dt", "hour"])

    if not df_moeda.empty:
        fig_price = go.Figure()
        fig_price.add_trace(go.Scatter(
            x=df_moeda["hour"],
            y=df_moeda["preco_medio"],
            mode="lines+markers",
            name="Preço Médio",
        ))
        fig_price.add_trace(go.Scatter(
            x=df_moeda["hour"],
            y=df_moeda["preco_max"],
            mode="lines",
            name="Máximo",
            line=dict(dash="dash"),
        ))
        fig_price.add_trace(go.Scatter(
            x=df_moeda["hour"],
            y=df_moeda["preco_min"],
            mode="lines",
            name="Mínimo",
            line=dict(dash="dash"),
        ))
        fig_price.update_layout(
            title=f"{selected.upper()}  Preço por Hora",
            xaxis_title="Hora",
            yaxis_title="USD",
        )
        st.plotly_chart(fig_price, use_container_width=True)

        c1, c2 = st.columns(2)
        with c1:
            fig_vol = px.bar(
                df_moeda, x="hour", y="volume_medio",
                title=f"{selected.upper()}  Volume Médio/Hora",
            )
            st.plotly_chart(fig_vol, use_container_width=True)
        with c2:
            fig_volat = px.line(
                df_moeda, x="hour", y="volatilidade_relativa",
                title=f"{selected.upper()}  Volatilidade Relativa (%)",
                markers=True,
            )
            st.plotly_chart(fig_volat, use_container_width=True)

st.markdown("---")
st.caption("Crypto Pipeline  Processamento de Dados Massivos  PUC Minas 2026/1")
