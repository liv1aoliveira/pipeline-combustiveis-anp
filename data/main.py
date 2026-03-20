from src.extract import extrair_bronze
from src.transform import transformar_silver
from src.load import gerar_gold

print("=" * 50)
print("🚀 PIPELINE ANP — PREÇOS DE COMBUSTÍVEIS")
print("=" * 50)

print("\n📦 ETAPA 1: EXTRAÇÃO (Bronze)")
print("-" * 30)
extrair_bronze()

print("\n🔄 ETAPA 2: TRANSFORMAÇÃO (Silver)")
print("-" * 30)
transformar_silver()

print("\n🏆 ETAPA 3: CARGA (Gold)")
print("-" * 30)
gerar_gold()

print("\n" + "=" * 50)
print("🎉 Pipeline concluído com sucesso!")
print("=" * 50)
