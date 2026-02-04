from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
import sqlite3
import os

# configurações
DB_NAME = "intuitive_care.db"
app = FastAPI(
    title="API Intuitive Care - Teste Nicolas",
    description="API para consulta de despesas de operadoras de saúde.",
    version="1.0.0"
)

# configuração de CORS 
# Sem isso, o navegador bloqueia a conexão do Frontend com o Backend.
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], # Em produção, colocariamos o dominio certo. Para teste, libera geral.
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

def get_db_connection():
    conn = sqlite3.connect(DB_NAME)
    conn.row_factory = sqlite3.Row # Isso permite acessar colunas pelo nome (ex: row['nome'])
    return conn

@app.get("/")
def read_root():
    return {"message": "API Online! Acesse /docs para ver a documentação."}

# listar operadoras
@app.get("/api/operadoras")
def listar_operadoras(
    busca: str = Query(None, description="Buscar por Razão Social"),
    page: int = Query(1, ge=1, description="Número da página"),
    limit: int = Query(10, ge=1, le=100, description="Itens por página")
):
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # lógica de Paginação (OFFSET)
    offset = (page - 1) * limit
    
    # Query Base
    query = "SELECT registro_ans, cnpj, razao_social, uf FROM operadoras"
    params = []
    
    # se tiver busca, adiciona o filtro WHERE
    if busca:
        query += " WHERE razao_social LIKE ?"
        params.append(f"%{busca}%") # % serve para buscar em qualquer parte do texto
    
    # ordenação e paginação
    query += " ORDER BY razao_social LIMIT ? OFFSET ?"
    params.extend([limit, offset])
    
    operadoras = cursor.execute(query, params).fetchall()
    conn.close()
    
    return {
        "data": [dict(op) for op in operadoras],
        "page": page,
        "limit": limit
    }

# detalhes da operadora
# banco usa registro_ans como chave principal.
# buscar pelo registro_ans que é mais seguro.
@app.get("/api/operadoras/{registro_ans}")
def detalhes_operadora(registro_ans: int):
    conn = get_db_connection()
    cursor = conn.cursor()
    
    operadora = cursor.execute(
        "SELECT * FROM operadoras WHERE registro_ans = ?", 
        (registro_ans,)
    ).fetchone()
    
    conn.close()
    
    if operadora is None:
        raise HTTPException(status_code=404, detail="Operadora não encontrada")
        
    return dict(operadora)

# histórico de despesas
@app.get("/api/operadoras/{registro_ans}/despesas")
def listar_despesas(registro_ans: int):
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # verifica se a operadora existe primeiro
    operadora = cursor.execute("SELECT razao_social FROM operadoras WHERE registro_ans = ?", (registro_ans,)).fetchone()
    if not operadora:
        conn.close()
        raise HTTPException(status_code=404, detail="Operadora não encontrada")

    despesas = cursor.execute(
        """
        SELECT ano, trimestre, valor_despesa, descricao 
        FROM despesas 
        WHERE registro_ans = ? 
        ORDER BY ano DESC, trimestre DESC
        """, 
        (registro_ans,)
    ).fetchall()
    
    conn.close()
    
    return {
        "operadora": operadora['razao_social'],
        "despesas": [dict(d) for d in despesas]
    }

# estatísticas agregadas
@app.get("/api/estatisticas")
def obter_estatisticas():
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Query 1: Total Geral
    total = cursor.execute("SELECT SUM(valor_despesa) FROM despesas").fetchone()[0]
    
    # Query 2: Média por Trimestre
    media = cursor.execute("SELECT AVG(valor_despesa) FROM despesas").fetchone()[0]
    
    # Query 3: Top 5 Estados mais caros
    top_estados = cursor.execute("""
        SELECT o.uf, SUM(d.valor_despesa) as total
        FROM despesas d
        JOIN operadoras o ON d.registro_ans = o.registro_ans
        GROUP BY o.uf
        ORDER BY total DESC
        LIMIT 5
    """).fetchall()
    
    conn.close()
    
    return {
        "total_geral": total,
        "media_despesa": media,
        "top_estados": [dict(e) for e in top_estados]
    }