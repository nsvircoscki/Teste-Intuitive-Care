import pandas as pd
import os
import random

# configurações
DIR_PROCESSED = "dados_processados"
DIR_RAW = "dados_brutos"
FILE_CONSOLIDADO = os.path.join(DIR_PROCESSED, "consolidado_despesas.csv")
FILE_MOCK_CADASTRO = os.path.join(DIR_RAW, "Relatorio_Cadop.csv")

def gerar_cadastro_fake():
    print("--- GERANDO CADASTRO MOCK (FICTÍCIO) ---")
    
    # lê as despesas para saber quais operadoras precisamos inventar
    if not os.path.exists(FILE_CONSOLIDADO):
        print(f"Erro: Arquivo consolidado não existe em: {FILE_CONSOLIDADO}")
        print("Rode o main.py primeiro!")
        return

    df_desp = pd.read_csv(FILE_CONSOLIDADO, sep=';', decimal=',', encoding='utf-8')
    
    # descobre o nome da coluna de registro
    col_reg = next((c for c in df_desp.columns if "REG" in c or "CD_OPS" in c), None)
    
    if not col_reg:
        print("Erro: Não achei coluna de registro nas despesas.")
        return

    # pega os códigos únicos que estão nas despesas
    codigos_existentes = df_desp[col_reg].unique()
    print(f"Encontrei {len(codigos_existentes)} operadoras no arquivo de despesas.")

    # 2. Cria dados fake para elas
    dados_fake = []
    estados = ['SP', 'RJ', 'MG', 'RS', 'SC', 'PR', 'BA', 'PE']
    
    for i, codigo in enumerate(codigos_existentes):
        dados_fake.append({
            'Registro_ANS': codigo,
            'Razao_Social': f"OPERADORA TESTE {i+1} LTDA",
            'CNPJ': f"00.000.000/{1000+i}-00",
            'UF': random.choice(estados),
            'Modalidade': 'Cooperativa Médica'
        })

    # 3. Salva como se fosse o arquivo oficial da ANS
    df_fake = pd.DataFrame(dados_fake)
    
    # cria a pasta se não existir
    os.makedirs(DIR_RAW, exist_ok=True)
    
    # salva com o separador e encoding que o script espera
    df_fake.to_csv(FILE_MOCK_CADASTRO, sep=';', index=False, encoding='latin1')
    
    print(f"SUCESSO! Arquivo MOCK criado em: {FILE_MOCK_CADASTRO}")
    print("Agora você pode rodar a transformação e o Join vai funcionar 100%.")

if __name__ == "__main__":
    gerar_cadastro_fake()