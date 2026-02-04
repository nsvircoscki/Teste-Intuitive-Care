import pandas as pd
import requests
import os
from io import StringIO
import urllib3

# configurações
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

DIR_RAW = "dados_brutos"
DIR_PROCESSED = "dados_processados"
FILE_CONSOLIDADO = os.path.join(DIR_PROCESSED, "consolidado_despesas.csv")
FILE_CADASTRO_LOCAL = os.path.join(DIR_RAW, "Relatorio_Cadop.csv")
URL_CADASTRO = "https://dadosabertos.ans.gov.br/FTP/PDA/operadoras_de_plano_de_saude_ativas/Relatorio_Cadop.csv"

def encontrar_coluna_inteligente(df, possiveis_nomes):
    """ Busca coluna por palavra-chave (ignora maiúsculas/minúsculas) """
    colunas_map = {c.upper().strip(): c for c in df.columns}
    for nome_upper in colunas_map:
        for chave in possiveis_nomes:
            if chave in nome_upper:
                return colunas_map[nome_upper]
    return None

def obter_cadastro_operadoras():
    """
    Lógica Híbrida:
    1. Verifica se já existe o arquivo local (criado pelo mock ou download anterior).
    2. Se não existir, tenta baixar da ANS.
    """
    print("\n--- Obtendo Cadastro de Operadoras ---")
    
    # TENTA LER LOCAL (Prioridade para o Mock/Cache)
    if os.path.exists(FILE_CADASTRO_LOCAL):
        print(f"-> Arquivo encontrado localmente: {FILE_CADASTRO_LOCAL}")
        try:
            # Tenta ler com ponto e vírgula
            df = pd.read_csv(FILE_CADASTRO_LOCAL, sep=';', on_bad_lines='skip', encoding='latin1')
            
            if df.shape[1] < 2:
                 df = pd.read_csv(FILE_CADASTRO_LOCAL, sep=',', on_bad_lines='skip', encoding='latin1')
            
            print("-> Leitura local: SUCESSO.")
            return df
        except Exception as e:
            print(f"-> Erro ao ler arquivo local: {e}. Tentando baixar...")

    # TENTA BAIXAR (Fallback se não tiver local)
    print("-> Arquivo local não existe. Tentando download da ANS...")
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        }
        response = requests.get(URL_CADASTRO, headers=headers, verify=False, timeout=60)
        
        if response.status_code == 200:
            # Salva o download para não precisar baixar de novo
            with open(FILE_CADASTRO_LOCAL, 'wb') as f:
                f.write(response.content)
            print(f"-> Download salvo em {FILE_CADASTRO_LOCAL}")
            
            df = pd.read_csv(StringIO(response.text), sep=';', on_bad_lines='skip', encoding='latin1')
            if df.shape[1] < 2:
                 df = pd.read_csv(StringIO(response.text), sep=',', on_bad_lines='skip', encoding='latin1')
            return df
        else:
            print(f"-> Falha no Download. Status Code: {response.status_code}")
            return None

    except Exception as e:
        print(f"-> Erro crítico de conexão: {e}")
        return None

def main():
    print("--- INICIANDO FASE 2: TRANSFORMAÇÃO ---")

    if not os.path.exists(FILE_CONSOLIDADO):
        print(f"Erro: Arquivo {FILE_CONSOLIDADO} não existe. Rode o main.py primeiro.")
        return

    df_despesas = pd.read_csv(FILE_CONSOLIDADO, sep=';', decimal=',', encoding='utf-8')
    
    # Identifica colunas dinamicamente
    col_chave_desp = encontrar_coluna_inteligente(df_despesas, ["REG", "CD_OPS", "CNPJ"])
    col_valor = encontrar_coluna_inteligente(df_despesas, ["VALOR", "SALDO", "DESPESA"])
    
    print(f"Colunas Despesas -> ID: '{col_chave_desp}' | Valor: '{col_valor}'")

    if not col_chave_desp or not col_valor:
        print("Erro: Colunas não identificadas no arquivo de despesas.")
        return

    # Limpeza de valores
    df_despesas[col_valor] = pd.to_numeric(df_despesas[col_valor], errors='coerce')
    df_despesas = df_despesas[df_despesas[col_valor] > 0]

    # OBTER CADASTRO (Local ou Download)
    df_cadastro = obter_cadastro_operadoras()
    
    if df_cadastro is not None:
        # Identifica colunas do cadastro
        col_chave_cad = encontrar_coluna_inteligente(df_cadastro, ["REGISTRO", "REG", "CD_OPS"])
        col_razao = encontrar_coluna_inteligente(df_cadastro, ["RAZAO", "NOME", "DENOMINACAO"])
        col_uf = encontrar_coluna_inteligente(df_cadastro, ["UF", "ESTADO"])

        print(f"Colunas Cadastro -> ID: '{col_chave_cad}' | Nome: '{col_razao}' | UF: '{col_uf}'")

        if col_chave_cad and col_razao:
            # Prepara chaves para numérico
            df_despesas[col_chave_desp] = pd.to_numeric(df_despesas[col_chave_desp], errors='coerce')
            df_cadastro[col_chave_cad] = pd.to_numeric(df_cadastro[col_chave_cad], errors='coerce')

            # Renomeia para padronizar
            df_cadastro = df_cadastro.rename(columns={col_razao: 'Razao_Social', col_uf: 'UF'})

            print("Cruzando dados (Merge)...")
            df_final = pd.merge(
                df_despesas,
                df_cadastro[[col_chave_cad, 'Razao_Social', 'UF']],
                left_on=col_chave_desp,
                right_on=col_chave_cad,
                how='left'
            )
            
            # Preenche quem não deu match
            df_final['Razao_Social'] = df_final['Razao_Social'].fillna('OPERADORA DESCONHECIDA')
            df_final['UF'] = df_final['UF'].fillna('ND')
        else:
            print("Aviso: Colunas vitais não encontradas no cadastro.")
            df_final = df_despesas
            df_final['Razao_Social'] = "ERRO COLUNAS"
            df_final['UF'] = "ND"
    else:
        print("Erro: Não foi possível obter nenhum cadastro (nem local, nem online).")
        df_final = df_despesas
        df_final['Razao_Social'] = "SEM CADASTRO"
        df_final['UF'] = "ND"

    # AGREGAÇÃO E SALVAMENTO
    if 'UF' not in df_final.columns: df_final['UF'] = 'ND'
    
    print("Gerando estatísticas...")
    agregado = df_final.groupby(['Razao_Social', 'UF'])[col_valor].agg(
        Total_Despesas='sum',
        Media_Trimestral='mean',
        Desvio_Padrao='std'
    ).reset_index().sort_values(by='Total_Despesas', ascending=False)
    
    arquivo_saida = os.path.join(DIR_PROCESSED, "despesas_agregadas.csv")
    agregado.to_csv(arquivo_saida, index=False, sep=';', decimal=',')
    
    print(f"\n✅ SUCESSO! Arquivo salvo em: {arquivo_saida}")
    print("Top 3 Operadoras:")
    print(agregado.head(3))

if __name__ == "__main__":
    main()