import pandas as pd
import sidrapy
import requests
import os

# 1. Configurações Iniciais e Códigos dos Municípios (IBGE)
# Lista: Juazeiro, Crato, Barbalha, Jardim, Missão Velha,
# Caririaçu, Farias Brito, Nova Olinda e Santana do Cariri.
municipios_codes = "2307304,2304202,2301901,2307106,2308401,2303204,2304301,2309201,2312106"

def get_base_producao_agricola():
    """Coleta dados da Tabela 839 do SIDRA (Milho) com códigos corrigidos"""
    print("Coletando Base 1: Produção Agrícola (Tabela 839)...")

    # v109 (Plantada), v216 (Colhida), v214 (Produzida), v112 (Rendimento)
    try:
        df = sidrapy.get_table(
            table_code="839",
            territorial_level="6",
            ibge_territorial_code=municipios_codes, # Corrected parameter name
            period="2014-2023",
            variable="109,216,214,112"
        )

        # Limpeza de Metadados (Remover a primeira linha que contém descrições)
        df.columns = df.iloc[0]
        df = df.iloc[1:]

        # Treatment of missing values and conversion to numeric
        # SIDRA uses '...' for unavailable data
        # Identify the value column, assuming it's the last column after metadata cleaning
        value_col_name = df.columns[-1]
        df[value_col_name] = pd.to_numeric(df[value_col_name].replace(['...', '-'], '0'), errors='coerce')

        df.to_csv("base1_producao_milho_corrigida.csv", index=False)
        print("Base 1 salva com sucesso com códigos 109, 216, 214 e 112.")
        return df

    except Exception as e:
        print(f"Erro na coleta da Tabela 839: {e}")
