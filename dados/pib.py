import sidrapy
from producao_agricola import municipios_codes

def get_base_pib():
    """Coleta dados de PIB Municipal (Série 2010-2023)"""
    print("Coletando Base 3: Indicadores Socioeconômicos (PIB)...")
    # Tabela 5938 - PIB a preços correntes e PIB per capita
    try:
        df = sidrapy.get_table(
            table_code="5938",
            territorial_level="6",
            ibge_territorial_code=municipios_codes,
            period="2014-2021", # Nota: PIB tem defasagem de 2 anos no IBGE
            variable="37" # Removed '39' as it was causing an error
        )
        df.to_csv("base3_pib_socioeconomico.csv", index=False)
        print("Base 3 salva com sucesso.")
        return df
    except Exception as e:
        print(f"Erro na coleta da Tabela 5938 (PIB): {e}")