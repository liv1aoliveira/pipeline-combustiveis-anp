import pandas as pd
import glob


def transformar_silver():
    """Limpa e padroniza os dados da camada bronze."""
    arquivos = glob.glob("data/bronze/*.csv")

    if not arquivos:
        print("❌ Nenhum .csv encontrado em data/bronze/")
        return

    dfs = []
    for arquivo in arquivos:
        try:
            df = pd.read_csv(arquivo, encoding="utf-8", low_memory=False)
            dfs.append(df)
            print(f"📥 Lido: {arquivo} | {len(df)} linhas")
        except Exception as e:
            print(f"❌ Erro ao ler {arquivo}: {e}")

    df_total = pd.concat(dfs, ignore_index=True)
    print(f"\n🔄 Total de linhas brutas: {len(df_total)}")

    # Padroniza nomes de colunas removendo acentos e caracteres especiais
    df_total.columns = (
        df_total.columns
        .str.strip()
        .str.lower()
        .str.normalize("NFKD")
        .str.encode("ascii", errors="ignore")
        .str.decode("utf-8")
        .str.replace(" ", "_", regex=False)
        .str.replace(r"[^\w]", "_", regex=True)
    )

    print(f"📋 Colunas normalizadas: {list(df_total.columns)}")

    # Remove linhas sem preço médio
    df_total = df_total.dropna(subset=["preco_medio_revenda"])

    # Converte tipos
    df_total["mes"] = pd.to_datetime(df_total["mes"], errors="coerce")
    df_total["preco_medio_revenda"] = pd.to_numeric(
        df_total["preco_medio_revenda"],  errors="coerce")
    df_total["preco_minimo_revenda"] = pd.to_numeric(
        df_total["preco_minimo_revenda"], errors="coerce")
    df_total["preco_maximo_revenda"] = pd.to_numeric(
        df_total["preco_maximo_revenda"], errors="coerce")

    # Remove linhas onde conversão falhou
    df_total = df_total.dropna(subset=["preco_medio_revenda", "mes"])

    df_total.to_csv("data/silver/combustiveis_silver.csv",
                    index=False, encoding="utf-8")
    print(f"\n✅ Silver gerado: {len(df_total)} linhas limpas")
    return df_total
