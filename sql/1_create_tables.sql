CREATE TABLE IF NOT EXISTS operadoras (
    registro_ans INTEGER PRIMARY KEY,
    cnpj TEXT,
    razao_social TEXT,
    uf TEXT,
    modalidade TEXT
);

CREATE TABLE IF NOT EXISTS despesas (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    registro_ans INTEGER,
    trimestre TEXT,
    ano INTEGER,
    valor_despesa REAL,
    descricao TEXT,
    arquivo_origem TEXT,
    FOREIGN KEY (registro_ans) REFERENCES operadoras(registro_ans)
);

CREATE INDEX IF NOT EXISTS idx_operadora_uf ON operadoras(uf);
CREATE INDEX IF NOT EXISTS idx_despesa_ano_tri ON despesas(ano, trimestre);