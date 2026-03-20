import pandas as pd
from sqlalchemy import create_engine


def gerar_gold():
    """Agrega os dados silver e salva a camada gold."""
    try:
        df = pd.read_csv(
            "data/silver/combustiveis_silver.csv", low_memory=False)
    except FileNotFoundError:
        print("❌ Arquivo silver não encontrado. Rode primeiro a transformação!")
        return

    print(f"📥 Silver carregado: {len(df)} linhas")

    df["mes"] = pd.to_datetime(df["mes"], errors="coerce")
    df["ano"] = df["mes"].dt.year.astype("Int64")
    df["mes_ano"] = df["mes"].dt.to_period("M").astype(str)

    gold = (
        df.groupby(["mes_ano", "ano", "produto", "estado"])
        .agg(
            preco_medio=("preco_medio_revenda",  "mean"),
            preco_minimo=("preco_minimo_revenda", "min"),
            preco_maximo=("preco_maximo_revenda", "max"),
            qtd_municipios=("municipio",            "nunique"),
            qtd_postos=("numero_de_postos_pesquisados", "sum"),
        )
        .reset_index()
        .round(3)
    )

    gold.to_csv("data/gold/precos_agregados.csv",
                index=False, encoding="utf-8")

    engine = create_engine("sqlite:///data/combustiveis.db")
    gold.to_sql("precos_agregados", engine, if_exists="replace", index=False)

    print(f"✅ Gold gerado: {len(gold)} linhas agregadas")
    print(f"   Período: {gold['mes_ano'].min()} até {gold['mes_ano'].max()}")
    print(f"   Produtos: {sorted(gold['produto'].unique())}")
    print(f"\n📊 Prévia:")
    print(gold.head(10).to_string())
    return gold
