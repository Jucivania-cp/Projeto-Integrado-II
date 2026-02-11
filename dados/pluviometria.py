import pandas as pd
import requests
import zipfile
import os

def baixar_e_extrair_funceme(url_zip, pasta_destino="dados_funceme"):
    """
    Baixa o arquivo ZIP da FUNCEME e extrai seu conteúdo automaticamente.
    """
    arquivo_zip = "postos.zip"

    print(f"Iniciando o download de: {url_zip}...")

    try:
        # 1. Faz o download do arquivo
        response = requests.get(url_zip, stream=True)
        response.raise_for_status() # Verifica se o link está ativo

        with open(arquivo_zip, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)

        print("Download concluído! Extraindo arquivos...")

        # 2. Extrai o conteúdo do ZIP
        if not os.path.exists(pasta_destino):
            os.makedirs(pasta_destino)

        with zipfile.ZipFile(arquivo_zip, 'r') as zip_ref:
            zip_ref.extractall(pasta_destino)

        # 3. Limpeza: remove o arquivo .zip após extrair
        os.remove(arquivo_zip)

        print(f"Arquivos extraídos com sucesso na pasta: '{pasta_destino}'")
        return pasta_destino

    except Exception as e:
        print(f"Erro ao automatizar o download: {e}")
        return None

# Execução automática
url = "https://cdn.funceme.br/calendario/postos/postos.zip"
pasta_dos_csvs = baixar_e_extrair_funceme(url)

def processar_arquivos_por_id(caminho_pasta):
    # Dicionário de mapeamento (Exemplo: preencha com os IDs que você encontrar no site)
    # Formato: 'ID_DO_ARQUIVO': 'NOME_DO_MUNICIPIO'
    mapeamento_cariri = {
        '78': 'JUAZEIRO DO NORTE',
        '43': 'CRATO',
        '20': 'BARBALHA',
        '76': 'JARDIM',
        '91': 'MISSAO VELHA',
        '33': 'CARIRIACU',
        '45': 'FARIAS BRITO',
        '99': 'NOVA OLINDA',
        '131': 'SANTANA DO CARIRI'
    }

    lista_dfs = []

    print("Iniciando leitura dos arquivos por ID...")

    for id_posto, nome_cidade in mapeamento_cariri.items():
        arquivo_nome = f"{id_posto}.txt"
        caminho_completo = os.path.join(caminho_pasta, arquivo_nome)

        if os.path.exists(caminho_completo):
            # Ler o TXT
            df_temp = pd.read_csv(caminho_completo, sep=';', encoding='latin-1')
            # Diagnóstico: Imprimir os nomes das colunas
            print(f"Colunas em {arquivo_nome} ({nome_cidade}): {df_temp.columns.tolist()}")

            # Adicionar coluna com o nome do município para identificação
            df_temp['Municipio'] = nome_cidade
            lista_dfs.append(df_temp)
            print(f"Arquivo {arquivo_nome} ({nome_cidade}) carregado.")
        else:
            print(f"Aviso: Arquivo {arquivo_nome} não encontrado para {nome_cidade}.")

    if lista_dfs:
        df_raw = pd.concat(lista_dfs, ignore_index=True)

        # Melt the 'Dia' columns into a single column
        id_vars = ['Municipios', 'Postos', 'Latitude', 'Longitude', 'Anos', 'Meses', 'Total', 'Municipio']
        value_vars = [col for col in df_raw.columns if col.startswith('Dia') and col[3:].isdigit()]

        df_melted = df_raw.melt(
            id_vars=id_vars,
            value_vars=value_vars,
            var_name='Dia_str',
            value_name='valor_chuva_mm'
        )

        # Extract day number from 'Dia_str' (e.g., 'Dia1' -> '1')
        df_melted['Dia'] = df_melted['Dia_str'].str.replace('Dia', '', regex=False).astype(int)

        # Create 'Data' column
        # Pad month and day with leading zeros for proper date parsing
        df_melted['Data'] = pd.to_datetime(
            df_melted['Anos'].astype(str) + '-' +
            df_melted['Meses'].astype(str).str.zfill(2) + '-' +
            df_melted['Dia'].astype(str).str.zfill(2),
            errors='coerce' # Coerce errors to NaT for invalid dates (e.g., Feb 30)
        )

        # Filter out rows with invalid dates (NaT)
        df_melted.dropna(subset=['Data'], inplace=True)

        # Filter period from 2014 to 2024
        df_final_local = df_melted[(df_melted['Data'].dt.year >= 2014) & (df_melted['Data'].dt.year <= 2024)]

        # Select and reorder relevant columns
        df_final_local = df_final_local[['Data', 'Municipio', 'valor_chuva_mm']]

        return df_final_local
    else:
        return None

#Processamento dos arquivos
if pasta_dos_csvs:
    # Caso os CSVs estejam em uma subpasta 'postos' dentro da extração:
    caminho_real = os.path.join(pasta_dos_csvs, "postos")
    if not os.path.exists(caminho_real):
        caminho_real = pasta_dos_csvs # Se não houver subpasta, usa a principal

    df_pluviometria = processar_arquivos_por_id(caminho_real)

    #if df_final is not None:
        #print("\nBase Pluviométrica do Cariri gerada com sucesso!")
        #display(df_final.head())
    #else:
        #print("\nErro: Nenhum dado foi processado. Verifique os IDs e o caminho dos arquivos.")