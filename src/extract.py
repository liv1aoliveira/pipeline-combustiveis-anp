import pandas as pd
import os
import glob

def extrair_bronze():
    """Leitura de todos os .xlsx da ANP e realiza o salvamento como .csv em bronze."""
    arquivos = glob.glob("data/bronze/*.xlsx")

    if not arquivos:
        print("❌ Nenhum arquivo .xlsx encontrado em data/bronze/")
        return

    for arquivo in arquivos:
        try:
            print(f"📥 Lendo: {os.path.basename(arquivo)}")
            # header=16 porque pandas conta a partir do 0 (linha 17 = índice 16)
            df = pd.read_excel(arquivo, header=16)
            # Remove linhas completamente vazias
            df = df.dropna(how="all")
            nome_saida = os.path.basename(arquivo).replace(".xlsx", ".csv")
            df.to_csv(f"data/bronze/{nome_saida}", index=False, encoding="utf-8")
            print(f"✅ Bronze gerado: {nome_saida} | {len(df)} linhas")
        except Exception as e:
            print(f"❌ Erro ao processar {arquivo}: {e}")