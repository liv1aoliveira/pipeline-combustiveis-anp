# 🛢️ Pipeline de Preços de Combustíveis — ANP

Pipeline ETL construído em Python com arquitetura **Medallion (Bronze → Silver → Gold)**, processando dados históricos de preços de combustíveis da ANP (Agência Nacional do Petróleo) de **2016 a 2025**.

## 📊 Sobre os dados

- **Fonte:** [ANP — Série Histórica de Preços](https://www.gov.br/anp/pt-br/assuntos/precos-e-defesa-da-concorrencia/precos/precos-revenda-e-de-distribuicao-combustiveis/serie-historica-do-levantamento-de-precos) 
- **Volume:** ~281.840 registros brutos | 19.824 registros agregados
- **Período:** Janeiro/2016 a Dezembro/2025
- **Produtos:** Gasolina Comum, Gasolina Aditivada, Etanol Hidratado, Óleo Diesel, Óleo Diesel S10, GLP, GNV
- **Granularidade:** Preços médios mensais por município e estado

## 🏗️ Arquitetura Medallion
```
Bronze  →  dados brutos convertidos de .xlsx para .csv
Silver  →  dados limpos, tipados e padronizados (281.840 linhas)
Gold    →  agregação mensal por produto e estado (19.824 linhas)
            salvo em CSV + banco SQLite
```

## 🛠️ Tecnologias

| Ferramenta | Uso |
|---|---|
| Python 3.12 | Linguagem principal |
| Pandas | Manipulação e transformação de dados |
| OpenPyXL | Leitura dos arquivos .xlsx da ANP |
| SQLAlchemy | Carga no banco de dados SQLite |
| SQLite | Armazenamento da camada Gold |
| Git | Controle de versão |

## 📁 Estrutura do Projeto
```
pipeline-combustiveis-anp/
│
├── data/
│   ├── bronze/    ← arquivos brutos convertidos para .csv
│   ├── silver/    ← dados limpos e padronizados
│   └── gold/      ← dados agregados prontos para análise
│
├── src/
│   ├── extract.py     ← leitura dos .xlsx e geração do bronze
│   ├── transform.py   ← limpeza e padronização (silver)
│   └── load.py        ← agregação e carga (gold + SQLite)
│
├── main.py        ← orquestra o pipeline completo
└── requirements.txt
```

## ▶️ Como executar
```bash
# 1. Clone o repositório
git clone https://github.com/liv1aoliveira/pipeline-combustiveis-anp

# 2. Crie e ative o ambiente virtual
python -m venv venv
venv\Scripts\activate

# 3. Instale as dependências
pip install -r requirements.txt

# 4. Coloque os arquivos .xlsx da ANP em data/bronze/

# 5. Execute o pipeline
python main.py
```

## 📈 Exemplo de saída (Gold)

| mes_ano | produto | estado | preco_medio | preco_minimo | preco_maximo | qtd_municipios |
|---|---|---|---|---|---|---|
| 2024-01 | GASOLINA COMUM | SAO PAULO | 5.487 | 4.990 | 6.200 | 89 |
| 2024-01 | ETANOL HIDRATADO | GOIAS | 3.124 | 2.800 | 3.490 | 34 |