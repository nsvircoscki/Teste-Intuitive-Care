import os
import requests
import zipfile
import pandas as pd
from io import BytesIO
import urllib3

# configurações
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
}

BASE_URL = "https://dadosabertos.ans.gov.br/FTP/PDA/demonstracoes_contabeis/"
DIR_RAW = "dados_brutos"
DIR_PROCESSED = "dados_processados"
ANOS = ["2023"] 
TRIMESTRES = ["1T", "2T", "3T"]

os.makedirs(DIR_RAW, exist_ok=True)
os.makedirs(DIR_PROCESSED, exist_ok=True)

def baixar_e_extrair(ano, trimestre):
    print(f"\n--- Buscando: {ano} / {trimestre} ---")
    
    nomes_possiveis = [
        f"{trimestre}{ano}.zip",           # Ex: 1T2023.zip (Padrão atual)
        f"{trimestre} {ano}.zip",          # Com espaço
        f"Demonstracoes_Contabeis_{trimestre}{ano}.zip",
    ]

    sucesso = False
    for nome in nomes_possiveis:
        url = f"{BASE_URL}{ano}/{nome}"
        print(f"Tentando: {url}")
        
        try:
            response = requests.get(url, headers=HEADERS, verify=False, timeout=30)
            
            if response.status_code == 200:
                print("✅ Arquivo encontrado! Baixando...")
                try:
                    with zipfile.ZipFile(BytesIO(response.content)) as z:
                        z.extractall(DIR_RAW)
                        print(f"   Extraído em {DIR_RAW}")
                        sucesso = True
                        break 
                except zipfile.BadZipFile:
                    print("❌ Erro: Arquivo corrompido.")
            elif response.status_code == 404:
                pass # Tenta o próximo nome silenciosamente
            else:
                print(f"⚠️ Status {response.status_code}")
                
        except Exception as e:
            print(f"Erro de conexão: {e}")

    if not sucesso:
        print(f"❌ ALERTA: Não foi possível achar o arquivo de {trimestre}/{ano}")

def normalizar_arquivo(arquivo_path):
    # Função para limpar e filtrar os dados
    try:
        # Tenta ler com encoding latin1 (padrão Brasil antigo)
        df = pd.read_csv(arquivo_path, sep=';', encoding='latin1', on_bad_lines='skip')
        df.columns = [c.strip().upper() for c in df.columns]
        
        # Procura coluna que tenha 'DESC' (Descricao)
        col_desc = next((c for c in df.columns if 'DESC' in c), None)
        
        if col_desc:
            # Filtra apenas o que é EVENTO ou SINISTRO
            filtro = df[col_desc].str.contains("EVENTO|SINISTRO", na=False, case=False)
            return df[filtro].copy()
        return None
    except Exception:
        return None

def main():
    # ETAPA 1: DOWNLOAD
    for ano in ANOS:
        for tri in TRIMESTRES:
            baixar_e_extrair(ano, tri)
            
    # ETAPA 2: CONSOLIDAÇÃO
    print("\n--- Iniciando Consolidação ---")
    todos = []
    
    # Lista arquivos CSV que foram baixados
    for root, dirs, files in os.walk(DIR_RAW):
        for file in files:
            if file.lower().endswith(".csv"):
                caminho_completo = os.path.join(root, file)
                print(f"Processando: {file}")
                
                df_temp = normalizar_arquivo(caminho_completo)
                if df_temp is not None:
                    # Adiciona coluna para saber de qual arquivo veio
                    df_temp['ARQUIVO_ORIGEM'] = file
                    todos.append(df_temp)
    
    if todos:
        df_final = pd.concat(todos, ignore_index=True)
        
        # Limpeza básica de valores numéricos (trocar vírgula por ponto)
        # Procura coluna de VALOR ou SALDO
        col_valor = next((c for c in df_final.columns if 'SALDO' in c or 'VALOR' in c), None)
        if col_valor:
            df_final[col_valor] = (
                df_final[col_valor]
                .astype(str)
                .str.replace('.', '', regex=False)
                .str.replace(',', '.', regex=False)
            )
            # Remove o que não for número
            df_final[col_valor] = pd.to_numeric(df_final[col_valor], errors='coerce')
        
        # Salva o arquivo final
        destino = f"{DIR_PROCESSED}/consolidado_despesas.csv"
        df_final.to_csv(destino, index=False, sep=';', decimal=',')
        print(f"\n🏆 SUCESSO! Arquivo gerado: {destino}")
        print(f"Total de linhas processadas: {len(df_final)}")
    else:
        print("\nNenhum dado foi processado. Verifique se os downloads funcionaram.")

if __name__ == "__main__":
    main()