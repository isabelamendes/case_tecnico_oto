WITH rfv_metricas AS (
    /*
    Cálculo das métricas de Recência (R), Frequência (F) e Valor (V) por cliente
        Recência: Dias desde a última compra
        Frequência: Quantidade de compras em um determinado período
        Valor: Valor total em compras no período
    Período deve ser definido pelo negócio (optei pelos últimos 5 anos)
    */
    SELECT
        cliente
        , DATE_DIFF(CURRENT_DATE(), MAX(data), DAY) AS recencia
        , COUNT(*) AS frequencia
        , SUM(valor) AS valor_total
    FROM `casetecnicooto.compras_rfv.compras`
    WHERE data >= DATE_SUB(CURRENT_DATE(), INTERVAL 5 YEAR)
    GROUP BY cliente
),

rfv_metricas_scores AS (
    /* 
    Atribuição de score (1 a 5) para cada métrica RFV
        Recência: quanto mais recente, maior o score
        Frequência e Valor: quanto maior, maior o score
    Condições devem ser definidas por regras de negócio
    */
    SELECT
        cliente
        , recencia
        , frequencia
        , valor_total
        -- Pontuação da Recência (quanto mais recente, maior o score)
        , CASE
            WHEN recencia <= 180 THEN 5
            WHEN recencia <= 365 THEN 4
            WHEN recencia <= 730 THEN 3
            WHEN recencia <= 1095 THEN 2
            ELSE 1
        END AS r_score
        -- Pontuação da Frequência (quanto mais compras, maior o score)
        , CASE
            WHEN frequencia >= 10 THEN 5
            WHEN frequencia >= 7 THEN 4
            WHEN frequencia >= 4 THEN 3
            WHEN frequencia >= 2 THEN 2
            ELSE 1
            END AS f_score
        -- Pontuação do Valor (quanto maior o valor gasto, maior o score)
        , CASE
            WHEN valor_total >= 520 THEN 5
            WHEN valor_total >= 430 THEN 4
            WHEN valor_total >= 350 THEN 3
            WHEN valor_total >= 190 THEN 2
            ELSE 1
        END AS v_score
    FROM rfv_metricas
)

SELECT
    /*
    Transformação dos scores em indicadores para 
        facilitar a segmentação, 
        identificação de clientes mais estratégicos, 
        entre outros.
    */
    *
    -- Classifica clientes em perfis específicos
    , CONCAT(CAST(r_score AS STRING), CAST(f_score AS STRING), CAST(v_score AS STRING)) AS rfv_score_concatenado
    -- Visão geral equilibrada entre R, F e V
    , ROUND((r_score + f_score + v_score) / 3, 2) AS rfv_score_medio
    --Visão ajustada ao peso estratégico de cada métrica.
    , ROUND(((r_score * 0.3) + (f_score * 0.3) + (v_score * 0.4)), 2) AS rfv_score_ponderado
FROM rfv_metricas_scores
ORDER BY rfv_score_ponderado DESC;