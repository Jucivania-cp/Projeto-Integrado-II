# Execução do pipeline
if __name__ == "__main__":
    try:
        base1 = get_base_producao_agricola()
        base3 = get_base_pib()
        base2 = processar_arquivos_por_id(caminho_real)
        print("\nPipeline concluído com sucesso! Arquivos CSV salvos no diretório.")
    except Exception as e:
        print(f"Erro na coleta: {e}")