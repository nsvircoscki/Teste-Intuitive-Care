WITH despesas_trimestrais AS (
    SELECT 
        d.registro_ans,
        o.razao_social,
        SUM(CASE WHEN d.trimestre = '1T' THEN d.valor_despesa ELSE 0 END) as despesa_t1,
        SUM(CASE WHEN d.trimestre = '3T' THEN d.valor_despesa ELSE 0 END) as despesa_t3
    FROM despesas d
    JOIN operadoras o ON d.registro_ans = o.registro_ans
    WHERE d.ano = 2023 -- Filtro do ano analisado
    GROUP BY d.registro_ans, o.razao_social
)
SELECT 
    razao_social,
    despesa_t1,
    despesa_t3,
    ((despesa_t3 - despesa_t1) / despesa_t1) * 100 as crescimento_percentual
FROM despesas_trimestrais
WHERE despesa_t1 > 0 -- Evita divisão por zero
ORDER BY crescimento_percentual DESC
LIMIT 5;

-- Distribuição por UF (Total e Média)
SELECT 
    o.uf,
    SUM(d.valor_despesa) as total_despesas,
    AVG(d.valor_despesa) as media_por_operadora
FROM despesas d
JOIN operadoras o ON d.registro_ans = o.registro_ans
GROUP BY o.uf
ORDER BY total_despesas DESC
LIMIT 5;

-- Operadoras com despesas acima da média em 2 ou mais trimestres
-- Calculamos a média geral por trimestre
WITH media_geral AS (
    SELECT trimestre, AVG(valor_despesa) as media_mercado
    FROM despesas
    GROUP BY trimestre
),
-- Marcamos quem ficou acima da média
performance AS (
    SELECT 
        d.registro_ans,
        d.trimestre,
        d.valor_despesa,
        m.media_mercado,
        CASE WHEN d.valor_despesa > m.media_mercado THEN 1 ELSE 0 END as acima_media
    FROM despesas d
    JOIN media_geral m ON d.trimestre = m.trimestre
)
-- Contamos quantas vezes cada operadora superou a média
SELECT 
    p.registro_ans,
    o.razao_social,
    SUM(p.acima_media) as qtd_trimestres_acima
FROM performance p
JOIN operadoras o ON p.registro_ans = o.registro_ans
GROUP BY p.registro_ans, o.razao_social
HAVING qtd_trimestres_acima >= 2;