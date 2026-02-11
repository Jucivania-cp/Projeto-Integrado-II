# Execução do pipeline
from dados.producao_agricola import get_base_producao_agricola
from dados.pib import get_base_pib
from dados.pluviometria import processar_arquivos_por_id

if __name__ == "__main__":
    try:
        base1 = get_base_producao_agricola()
        base3 = get_base_pib()
        base2 = processar_arquivos_por_id
        
        print("\nPipeline concluído com sucesso! Arquivos CSV salvos no diretório.")
    except Exception as e:
        print(f"Erro na coleta: {e}")