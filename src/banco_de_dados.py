import sqlite3
import pandas as pd
import os

# configurações
DB_NAME = "intuitive_care.db"
DIR_SQL = "sql"
DIR_PROCESSED = "dados_processados"
DIR_RAW = "dados_brutos"

def executar_script_sql(conn, arquivo_sql):
    with open(arquivo_sql, 'r', encoding='utf-8') as f:
        script = f.read()
        conn.executescript(script)

def encontrar_coluna_inteligente(df, possiveis_nomes):
    # busca coluna por palavra chave
    colunas_map = {c.upper().strip(): c for c in df.columns}
    for nome_upper in colunas_map:
        for chave in possiveis_nomes:
            if chave in nome_upper:
                return colunas_map[nome_upper]
    return None

def main():
    print("--- INICIANDO BANCO DE DADOS (NICOLAS) ---")
    
    # Conexão
    conn = sqlite3.connect(DB_NAME)
    print(f"Banco '{DB_NAME}' conectado.")

    # Criar Tabelas
    print("Criando tabelas...")
    try:
        executar_script_sql(conn, os.path.join(DIR_SQL, "1_create_tables.sql"))
    except Exception as e:
        print(f"Erro ao criar tabelas: {e}")
        return

    # Importar Dados
    print("Importando dados...")
    
    # Importar Operadoras (Mock ou Real)
    try:
        caminho_ops = os.path.join(DIR_RAW, "Relatorio_Cadop.csv")
        df_ops = pd.read_csv(caminho_ops, sep=';', encoding='latin1')
        if df_ops.shape[1] < 2: df_ops = pd.read_csv(caminho_ops, sep=',', encoding='latin1')
        
        # Mapeamento Inteligente
        col_reg = encontrar_coluna_inteligente(df_ops, ["REG", "CD_OPS"])
        col_nome = encontrar_coluna_inteligente(df_ops, ["RAZAO", "NOME", "DENOMINACAO"])
        col_uf = encontrar_coluna_inteligente(df_ops, ["UF", "ESTADO"])

        if col_reg and col_nome:
            df_ops_db = pd.DataFrame()
            df_ops_db['registro_ans'] = df_ops[col_reg]
            df_ops_db['razao_social'] = df_ops[col_nome]
            df_ops_db['uf'] = df_ops[col_uf] if col_uf else 'ND'
            df_ops_db['cnpj'] = df_ops.get('CNPJ', '000000') 
            df_ops_db['modalidade'] = df_ops.get('Modalidade', 'Desconhecida')

            df_ops_db.to_sql('operadoras', conn, if_exists='replace', index=False)
            print(f"-> Operadoras importadas: {len(df_ops_db)}")
        else:
            print("ERRO: Colunas não encontradas no arquivo de operadoras.")

    except Exception as e:
        print(f"Erro ao importar operadoras: {e}")

    # Importar Despesas
    try:
        caminho_desp = os.path.join(DIR_PROCESSED, "consolidado_despesas.csv")
        df_desp = pd.read_csv(caminho_desp, sep=';', decimal=',')
        
        c_reg = encontrar_coluna_inteligente(df_desp, ["REG", "CD_OPS"])
        c_val = encontrar_coluna_inteligente(df_desp, ["VALOR", "SALDO", "DESPESA"])
        c_ano = encontrar_coluna_inteligente(df_desp, ["ANO"])
        
        if c_reg and c_val:
            df_desp_db = pd.DataFrame()
            df_desp_db['registro_ans'] = df_desp[c_reg]
            df_desp_db['valor_despesa'] = df_desp[c_val]
            df_desp_db['ano'] = df_desp[c_ano] if c_ano else 2023
            
            # Se não tiver trimestre, define padrão '1T' para não quebrar
            c_tri = encontrar_coluna_inteligente(df_desp, ["TRIMESTRE", "TRI"])
            if c_tri:
                df_desp_db['trimestre'] = df_desp[c_tri]
            else:
                df_desp_db['trimestre'] = '1T'

            df_desp_db['descricao'] = 'DESPESA ASSISTENCIAL'
            
            df_desp_db.to_sql('despesas', conn, if_exists='replace', index=False)
            print(f"-> Despesas importadas: {len(df_desp_db)}")
        else:
            print(f"ERRO CRÍTICO: Não achei colunas REG ou VALOR. Colunas disponíveis: {df_desp.columns.tolist()}")

    except Exception as e:
        print(f"Erro ao importar despesas: {e}")

    # Teste Rápido
    print("\n--- RESULTADO FINAL (TOP 5) ---")
    try:
        query = """
        SELECT o.razao_social, o.uf, SUM(d.valor_despesa) as total
        FROM despesas d
        JOIN operadoras o ON d.registro_ans = o.registro_ans
        GROUP BY o.razao_social
        ORDER BY total DESC
        LIMIT 5
        """
        resultado = pd.read_sql_query(query, conn)
        print(resultado)
    except Exception as e:
        print(f"Erro na query de teste: {e}")

    conn.close()
    print("\n✅ BANCO DE DADOS PRONTO!")

if __name__ == "__main__":
    main()