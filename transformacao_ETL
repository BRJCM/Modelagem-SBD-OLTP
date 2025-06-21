-- Avaliação 02: Modelagem de Data Warehouse - Parte 2
-- Brian José Costa de Medeiro DRE: 121087678
-- Script de Transformação ETL

-- 1. Criação da Dim_Tempo
INSERT INTO dw.Dim_Tempo (
    data_completa, ano, trimestre, mes, nome_mes, dia_do_mes,
    dia_da_semana, nome_dia_da_semana, semana_do_ano, eh_fim_de_semana,
    hora_completa, hora, minuto
)
SELECT
    generate_series('2020-01-01'::timestamp, '2025-12-31'::timestamp, '1 hour')::date AS data_completa_date,
    EXTRACT(YEAR FROM generate_series('2020-01-01'::timestamp, '2025-12-31'::timestamp, '1 hour')) AS ano,
    EXTRACT(QUARTER FROM generate_series('2020-01-01'::timestamp, '2025-12-31'::timestamp, '1 hour')) AS trimestre,
    EXTRACT(MONTH FROM generate_series('2020-01-01'::timestamp, '2025-12-31'::timestamp, '1 hour')) AS mes,
    TO_CHAR(generate_series('2020-01-01'::timestamp, '2025-12-31'::timestamp, '1 hour'), 'Month') AS nome_mes,
    EXTRACT(DAY FROM generate_series('2020-01-01'::timestamp, '2025-12-31'::timestamp, '1 hour')) AS dia_do_mes,
    EXTRACT(DOW FROM generate_series('2020-01-01'::timestamp, '2025-12-31'::timestamp, '1 hour')) AS dia_da_semana,
    TO_CHAR(generate_series('2020-01-01'::timestamp, '2025-12-31'::timestamp, '1 hour'), 'Day') AS nome_dia_da_semana,
    EXTRACT(WEEK FROM generate_series('2020-01-01'::timestamp, '2025-12-31'::timestamp, '1 hour')) AS semana_do_ano,
    CASE WHEN EXTRACT(DOW FROM generate_series('2020-01-01'::timestamp, '2025-12-31'::timestamp, '1 hour')) IN (0, 6) THEN TRUE ELSE FALSE END AS eh_fim_de_semana,
    generate_series('2020-01-01'::timestamp, '2025-12-31'::timestamp, '1 hour')::time AS hora_completa,
    EXTRACT(HOUR FROM generate_series('2020-01-01'::timestamp, '2025-12-31'::timestamp, '1 hour')) AS hora,
    EXTRACT(MINUTE FROM generate_series('2020-01-01'::timestamp, '2025-12-31'::timestamp, '1 hour')) AS minuto
ON CONFLICT (data_completa) DO NOTHING; -- Evita duplicatas se executado múltiplas vezes

-- 1. Transformação e Carga da Dim_Cliente
-- Lógica de upsert para Dim_Cliente: Insere novos clientes ou atualiza existentes.
INSERT INTO dw.Dim_Cliente (
    cliente_id_oltp, nome_completo, cpf_cnpj, tipo_pessoa, email, telefone,
    cidade_cliente, estado_cliente, data_cadastro
)
SELECT
    sc.cliente_id,
    sc.nome_completo,
    sc.cpf_cnpj,
    sc.tipo_pessoa,
    sc.email,
    sc.telefone,
    sc.endereco_cidade,
    sc.endereco_estado,
    sc.data_cadastro
FROM staging.clientes sc
ON CONFLICT (cliente_id_oltp, tipo_pessoa) DO UPDATE SET -- Assume que cliente_id_oltp + tipo_pessoa é PK natural para evitar conflitos de PF/PJ com mesmo ID. Melhor seria cpf_cnpj
    nome_completo = EXCLUDED.nome_completo,
    email = EXCLUDED.email,
    telefone = EXCLUDED.telefone,
    cidade_cliente = EXCLUDED.cidade_cliente,
    estado_cliente = EXCLUDED.estado_cliente,
    data_cadastro = EXCLUDED.data_cadastro; -- Atualiza data de cadastro se necessário, ou pode ser ignorado se for apenas de inserção

-- 2. Transformação e Carga da Dim_Motorista
INSERT INTO dw.Dim_Motorista (
    motorista_id_oltp, sk_cliente_responsavel, nome_completo_motorista,
    cnh, cnh_categoria, cnh_validade
)
SELECT
    sm.motorista_id,
    dc.sk_cliente,
    sm.nome_completo,
    sm.cnh,
    sm.cnh_categoria,
    sm.cnh_validade
FROM staging.motoristas sm
JOIN dw.Dim_Cliente dc ON sm.cliente_id = dc.cliente_id_oltp AND sm.empresa_id = dc.empresa_id -- Adapte a junção se cliente_id_oltp não for globalmente único
ON CONFLICT (motorista_id_oltp) DO UPDATE SET
    sk_cliente_responsavel = EXCLUDED.sk_cliente_responsavel,
    nome_completo_motorista = EXCLUDED.nome_completo_motorista,
    cnh = EXCLUDED.cnh,
    cnh_categoria = EXCLUDED.cnh_categoria,
    cnh_validade = EXCLUDED.cnh_validade;

-- 3. Transformação e Carga da Dim_Patio
INSERT INTO dw.Dim_Patio (
    patio_id_oltp, nome_patio, endereco_patio, cidade_patio, estado_patio
)
SELECT
    sp.patio_id,
    sp.nome,
    sp.endereco,
    -- Tentativa de extrair cidade e estado do endereço. Ajuste conforme padrão real.
    CASE WHEN sp.endereco LIKE '%, % - %' THEN SPLIT_PART(SPLIT_PART(sp.endereco, ',', 2), '-', 1)::VARCHAR(100) ELSE NULL END AS cidade_patio,
    CASE WHEN sp.endereco LIKE '%, % - %' THEN SPLIT_PART(sp.endereco, '-', 2)::VARCHAR(50) ELSE NULL END AS estado_patio
FROM staging.patios sp
ON CONFLICT (patio_id_oltp) DO UPDATE SET
    nome_patio = EXCLUDED.nome_patio,
    endereco_patio = EXCLUDED.endereco_patio,
    cidade_patio = EXCLUDED.cidade_patio,
    estado_patio = EXCLUDED.estado_patio;

-- 4. Transformação e Carga da Dim_Veiculo
INSERT INTO dw.Dim_Veiculo (
    veiculo_id_oltp, placa, chassi, marca, modelo, cor,
    ano_fabricacao, cambio, possui_ar_cond, situacao_atual_oltp,
    nome_grupo_veiculo, descricao_grupo_veiculo, tarifa_diaria_base_grupo,
    origem_frota
)
SELECT
    sv.veiculo_id,
    sv.placa,
    sv.chassi,
    sv.marca,
    sv.modelo,
    sv.cor,
    sv.ano_fabricacao,
    sv.cambio,
    sv.possui_ar_cond,
    sv.situacao,
    sgv.nome_grupo,
    sgv.descricao_grupo,
    sgv.tarifa_diaria_base,
    -- Lógica para 'origem_frota': "Propria" se empresa_id for "Empresa Galeao", senão "Externa"
    CASE
        WHEN sv.empresa_id = 'Empresa Galeao' THEN 'Propria'
        ELSE 'Externa'
    END AS origem_frota
FROM staging.veiculos sv
JOIN staging.grupos_veiculos sgv ON sv.grupo_id = sgv.grupo_id AND sv.empresa_id = sgv.empresa_id
ON CONFLICT (veiculo_id_oltp) DO UPDATE SET
    placa = EXCLUDED.placa,
    chassi = EXCLUDED.chassi,
    marca = EXCLUDED.marca,
    modelo = EXCLUDED.modelo,
    cor = EXCLUDED.cor,
    ano_fabricacao = EXCLUDED.ano_fabricacao,
    cambio = EXCLUDED.cambio,
    possui_ar_cond = EXCLUDED.possui_ar_cond,
    situacao_atual_oltp = EXCLUDED.situacao_atual_oltp,
    nome_grupo_veiculo = EXCLUDED.nome_grupo_veiculo,
    descricao_grupo_veiculo = EXCLUDED.descricao_grupo_veiculo,
    tarifa_diaria_base_grupo = EXCLUDED.tarifa_diaria_base_grupo,
    origem_frota = EXCLUDED.origem_frota;
