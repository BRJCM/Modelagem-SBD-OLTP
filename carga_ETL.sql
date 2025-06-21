-- Avaliação 02: Modelagem de Data Warehouse - Parte 2
-- Brian José Costa de Medeiro DRE: 121087678
-- Script de Carga ETL para Tabelas de Fatos

-- 1. Carga da Fato_Locacao
INSERT INTO dw.Fato_Locacao (
    sk_tempo_retirada, sk_tempo_devolucao_prevista, sk_tempo_devolucao_real,
    sk_cliente, sk_motorista, sk_veiculo, sk_patio_retirada, sk_patio_devolucao,
    locacao_id_oltp, reserva_id_oltp, valor_previsto, valor_final,
    duracao_em_dias_prevista, duracao_em_dias_real, dias_restantes_para_devolucao
)
SELECT
    dt_retirada.sk_tempo AS sk_tempo_retirada,
    dt_devol_prev.sk_tempo AS sk_tempo_devolucao_prevista,
    dt_devol_real.sk_tempo AS sk_tempo_devolucao_real,
    dc.sk_cliente AS sk_cliente,
    dm.sk_motorista AS sk_motorista,
    dv.sk_veiculo AS sk_veiculo,
    dp_retirada.sk_patio AS sk_patio_retirada,
    dp_devolucao.sk_patio AS sk_patio_devolucao,
    sl.locacao_id AS locacao_id_oltp,
    sl.reserva_id AS reserva_id_oltp,
    sl.valor_previsto AS valor_previsto,
    sl.valor_final AS valor_final,
    -- Calcula duração prevista em dias
    EXTRACT(EPOCH FROM (sl.devolucao_prevista - sl.retirada_real)) / (24 * 3600) AS duracao_em_dias_prevista,
    -- Calcula duração real em dias (NULL se não devolvido)
    CASE
        WHEN sl.devolucao_real IS NOT NULL THEN EXTRACT(EPOCH FROM (sl.devolucao_real - sl.retirada_real)) / (24 * 3600)
        ELSE NULL
    END AS duracao_em_dias_real,
    -- Calcula dias restantes para devolução (apenas para locações ativas)
    CASE
        WHEN sl.devolucao_real IS NULL THEN EXTRACT(EPOCH FROM (sl.devolucao_prevista - NOW())) / (24 * 3600)
        ELSE NULL
    END AS dias_restantes_para_devolucao
FROM staging.locacoes sl
JOIN dw.Dim_Tempo dt_retirada ON sl.retirada_real::date = dt_retirada.data_completa
JOIN dw.Dim_Tempo dt_devol_prev ON sl.devolucao_prevista::date = dt_devol_prev.data_completa
LEFT JOIN dw.Dim_Tempo dt_devol_real ON sl.devolucao_real::date = dt_devol_real.data_completa
JOIN dw.Dim_Cliente dc ON sl.cliente_id = dc.cliente_id_oltp
JOIN dw.Dim_Motorista dm ON sl.motorista_id = dm.motorista_id_oltp
JOIN dw.Dim_Veiculo dv ON sl.veiculo_id = dv.veiculo_id_oltp
JOIN dw.Dim_Patio dp_retirada ON sl.patio_retirada_id = dp_retirada.patio_id_oltp
LEFT JOIN dw.Dim_Patio dp_devolucao ON sl.patio_devolucao_id = dp_devolucao.patio_id_oltp
ON CONFLICT (locacao_id_oltp) DO NOTHING; -- Evita duplicatas ao reexecutar

-- 2. Carga da Fato_Reserva
INSERT INTO dw.Fato_Reserva (
    sk_tempo_criacao_reserva, sk_tempo_retirada_prevista, sk_tempo_devolucao_prevista,
    sk_cliente, sk_veiculo_grupo, sk_patio_retirada,
    reserva_id_oltp, situacao_reserva_oltp,
    quantidade_reservas, duracao_em_dias_prevista, dias_ate_retirada_prevista
)
SELECT
    dt_criacao.sk_tempo AS sk_tempo_criacao_reserva,
    dt_retirada_prev.sk_tempo AS sk_tempo_retirada_prevista,
    dt_devol_prev_res.sk_tempo AS sk_tempo_devolucao_prevista,
    dc.sk_cliente AS sk_cliente,
    dv.sk_veiculo AS sk_veiculo_grupo, -- Ligação ao veículo (via grupo)
    dp_retirada_res.sk_patio AS sk_patio_retirada,
    sr.reserva_id AS reserva_id_oltp,
    sr.situacao_reserva AS situacao_reserva_oltp,
    1 AS quantidade_reservas, -- Cada linha é uma reserva
    -- Calcula duração prevista da reserva em dias
    EXTRACT(EPOCH FROM (sr.devolucao_prevista - sr.retirada_prevista)) / (24 * 3600) AS duracao_em_dias_prevista,
    -- Calcula dias até a retirada prevista
    EXTRACT(EPOCH FROM (sr.retirada_prevista - NOW())) / (24 * 3600) AS dias_ate_retirada_prevista
FROM staging.reservas sr
JOIN dw.Dim_Tempo dt_criacao ON sr.criado_em::date = dt_criacao.data_completa
JOIN dw.Dim_Tempo dt_retirada_prev ON sr.retirada_prevista::date = dt_retirada_prev.data_completa
JOIN dw.Dim_Tempo dt_devol_prev_res ON sr.devolucao_prevista::date = dt_devol_prev_res.data_completa
JOIN dw.Dim_Cliente dc ON sr.cliente_id = dc.cliente_id_oltp
JOIN dw.Dim_Veiculo dv ON sr.grupo_id = dv.veiculo_id_oltp -- Mapeia grupo da reserva para a Dim_Veiculo
LEFT JOIN staging.grupos_veiculos sgv_reserva ON sr.grupo_id = sgv_reserva.grupo_id AND sr.empresa_id = sgv_reserva.empresa_id
LEFT JOIN dw.Dim_Veiculo dv ON sgv_reserva.nome_grupo = dv.nome_grupo_veiculo -- Mapeia grupo da reserva para a Dim_Veiculo
JOIN dw.Dim_Patio dp_retirada_res ON sr.patio_retirada_id = dp_retirada_res.patio_id_oltp
WHERE dv.sk_veiculo IS NOT NULL -- Garante que o grupo foi encontrado na Dim_Veiculo
ON CONFLICT (reserva_id_oltp) DO NOTHING;

-- Correção para a junção da Fato_Reserva com Dim_Veiculo:
-- A reserva é por grupo de veículo, não por veículo específico.
-- A Dim_Veiculo contém atributos do grupo. Portanto, a junção deve ser feita pelo nome do grupo.

-- 3. Carga da Fato_Movimentacao_Patio
INSERT INTO dw.Fato_Movimentacao_Patio (
    sk_tempo_devolucao, sk_veiculo, sk_patio_origem, sk_patio_destino,
    locacao_id_oltp, quantidade_movimentos
)
SELECT
    dt_devol_mov.sk_tempo AS sk_tempo_devolucao,
    dv.sk_veiculo AS sk_veiculo,
    dp_retirada_mov.sk_patio AS sk_patio_origem,
    dp_devolucao_mov.sk_patio AS sk_patio_destino,
    sl.locacao_id AS locacao_id_oltp,
    1 AS quantidade_movimentos -- Cada linha é um movimento
FROM staging.locacoes sl
JOIN dw.Dim_Tempo dt_devol_mov ON sl.devolucao_real::date = dt_devol_mov.data_completa -- Apenas para locações devolvidas
JOIN dw.Dim_Veiculo dv ON sl.veiculo_id = dv.veiculo_id_oltp
JOIN dw.Dim_Patio dp_retirada_mov ON sl.patio_retirada_id = dp_retirada_mov.patio_id_oltp
JOIN dw.Dim_Patio dp_devolucao_mov ON sl.patio_devolucao_id = dp_devolucao_mov.patio_id_oltp
WHERE sl.devolucao_real IS NOT NULL -- Apenas movimentos completos (com devolução real)
ON CONFLICT (locacao_id_oltp) DO NOTHING; -- Para evitar duplicação em caso de reexecução
