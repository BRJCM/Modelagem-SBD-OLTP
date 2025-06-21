-- Avaliação 02: Modelagem de Data Warehouse - Parte 2
-- Brian José Costa de Medeiro DRE: 121087678
-- Script de Geração de Relatórios Gerenciais e Matriz de Percentuais de Movimentação

-- 12.a. Relatório: Controle de Pátio
-- Quantitativo de veículos no pátio por "grupo" e "origem".
-- Agrupamento por marca do veículo, modelos e tipo de mecanização.
-- Por “origem” entenda-se da frota da empresa dona do pátio, ou da frota das outras cinco empresas associadas.
SELECT
    dp.nome_patio,
    dv.nome_grupo_veiculo AS grupo_veiculo,
    dv.marca,
    dv.modelo,
    dv.cambio AS tipo_mecanizacao,
    dv.origem_frota, -- 'Propria' ou 'Externa'
    COUNT(fl.sk_veiculo) AS quantidade_veiculos_no_patio
FROM dw.Fato_Locacao fl
JOIN dw.Dim_Veiculo dv ON fl.sk_veiculo = dv.sk_veiculo
JOIN dw.Dim_Patio dp ON fl.sk_patio_devolucao = dp.sk_patio -- Veículos que foram devolvidos no pátio
JOIN dw.Dim_Tempo dt ON fl.sk_tempo_devolucao_real = dt.sk_tempo
WHERE fl.devolucao_real IS NOT NULL -- Considerando apenas veículos que foram devolvidos
AND fl.dias_restantes_para_devolucao IS NULL -- E que não estão mais em locação (já devolvidos)
AND dv.situacao_atual_oltp = 'Disponível' -- Assumindo que este campo reflete o status no pátio
-- Pode ser necessário ajustar a lógica para o "quantitativo de veículos no pátio"
-- dependendo de como o status 'Disponível' é gerenciado ou se há um fato de inventário de pátio.
-- Esta consulta mostra o histórico de devoluções por pátio. Para inventário atual,
-- precisaríamos de um snapshot da situação da vaga.
GROUP BY
    dp.nome_patio, dv.nome_grupo_veiculo, dv.marca, dv.modelo, dv.cambio, dv.origem_frota
ORDER BY
    dp.nome_patio, quantidade_veiculos_no_patio DESC;


-- 12.b. Relatório: Controle das Locações
-- Quantitativo de veículos alugados por “grupo”, e dimensão de tempo de locação e tempo restante para devolução.
SELECT
    dv.nome_grupo_veiculo AS grupo_veiculo,
    COUNT(fl.sk_locacao) AS total_alugueis_ativos,
    AVG(fl.duracao_em_dias_prevista) AS duracao_media_prevista_dias,
    AVG(fl.duracao_em_dias_real) AS duracao_media_real_dias, -- Inclui apenas locações concluídas
    AVG(fl.dias_restantes_para_devolucao) AS media_dias_restantes_devolucao
FROM dw.Fato_Locacao fl
JOIN dw.Dim_Veiculo dv ON fl.sk_veiculo = dv.sk_veiculo
WHERE fl.valor_final IS NULL -- Considera locações ainda em andamento (valor_final NULL)
GROUP BY
    dv.nome_grupo_veiculo
ORDER BY
    total_alugueis_ativos DESC;


-- 12.c. Relatório: Controle de Reservas
-- Quantas reservas por “grupo” de veículo, “pátio” (onde os clientes desejam retirar),
-- por tempo de retirada futura, e/ou tempo de duração das locações, e pelas cidades de origem dos clientes.
SELECT
    dv.nome_grupo_veiculo AS grupo_veiculo,
    dp.nome_patio AS patio_retirada,
    dc.cidade_cliente,
    fr.dias_ate_retirada_prevista, -- Tempo até a retirada futura
    fr.duracao_em_dias_prevista,   -- Tempo de duração das locações
    COUNT(fr.sk_reserva) AS total_reservas
FROM dw.Fato_Reserva fr
JOIN dw.Dim_Veiculo dv ON fr.sk_veiculo_grupo = dv.sk_veiculo
JOIN dw.Dim_Patio dp ON fr.sk_patio_retirada = dp.sk_patio
JOIN dw.Dim_Cliente dc ON fr.sk_cliente = dc.sk_cliente
WHERE fr.situacao_reserva_oltp = 'Ativa'
GROUP BY
    dv.nome_grupo_veiculo,
    dp.nome_patio,
    dc.cidade_cliente,
    fr.dias_ate_retirada_prevista,
    fr.duracao_em_dias_prevista
ORDER BY
    fr.dias_ate_retirada_prevista ASC, total_reservas DESC;


-- 12.d. Relatório: Quais os “grupos” de veículos mais alugados, cruzando, eventualmente, com a origem dos clientes.
SELECT
    dv.nome_grupo_veiculo AS grupo_veiculo,
    dc.cidade_cliente,
    COUNT(fl.sk_locacao) AS quantidade_alugueis
FROM dw.Fato_Locacao fl
JOIN dw.Dim_Veiculo dv ON fl.sk_veiculo = dv.sk_veiculo
JOIN dw.Dim_Cliente dc ON fl.sk_cliente = dc.sk_cliente
GROUP BY
    dv.nome_grupo_veiculo,
    dc.cidade_cliente
ORDER BY
    quantidade_alugueis DESC;


-- 13. Matriz de Percentuais de Movimentação entre Pátios (Cadeia de Markov)
-- Esta consulta gera a matriz estocástica com os percentuais de movimentação da frota
-- entre os pátios (pátio de retirada para pátio de devolução).
WITH MovimentosTotaisPorOrigem AS (
    SELECT
        dp_origem.nome_patio AS patio_origem,
        COUNT(fmp.sk_movimentacao_patio) AS total_movimentos_origem
    FROM dw.Fato_Movimentacao_Patio fmp
    JOIN dw.Dim_Patio dp_origem ON fmp.sk_patio_origem = dp_origem.sk_patio
    GROUP BY
        dp_origem.nome_patio
),
MovimentosEspecificos AS (
    SELECT
        dp_origem.nome_patio AS patio_origem,
        dp_destino.nome_patio AS patio_destino,
        COUNT(fmp.sk_movimentacao_patio) AS quantidade_movimentos
    FROM dw.Fato_Movimentacao_Patio fmp
    JOIN dw.Dim_Patio dp_origem ON fmp.sk_patio_origem = dp_origem.sk_patio
    JOIN dw.Dim_Patio dp_destino ON fmp.sk_patio_destino = dp_destino.sk_patio
    GROUP BY
        dp_origem.nome_patio, dp_destino.nome_patio
)
SELECT
    me.patio_origem,
    me.patio_destino,
    me.quantidade_movimentos,
    mt.total_movimentos_origem,
    ROUND((me.quantidade_movimentos::NUMERIC / mt.total_movimentos_origem) * 100, 2) AS percentual_movimento
FROM MovimentosEspecificos me
JOIN MovimentosTotaisPorOrigem mt ON me.patio_origem = mt.patio_origem
ORDER BY
    me.patio_origem, percentual_movimento DESC;
