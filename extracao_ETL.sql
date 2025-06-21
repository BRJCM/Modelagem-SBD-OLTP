-- Avaliação 02: Modelagem de Data Warehouse - Parte 2
-- Brian José Costa de Medeiro DRE: 121087678
-- Script de Extração ETL para Área Staging

-- Criação do esquema de Staging
CREATE SCHEMA IF NOT EXISTS staging;

-- Limpa as tabelas de staging (para execuções repetidas)
TRUNCATE TABLE staging.clientes RESTART IDENTITY CASCADE;
TRUNCATE TABLE staging.motoristas RESTART IDENTITY CASCADE;
TRUNCATE TABLE staging.patios RESTART IDENTITY CASCADE;
TRUNCATE TABLE staging.vagas RESTART IDENTITY CASCADE;
TRUNCATE TABLE staging.grupos_veiculos RESTART IDENTITY CASCADE;
TRUNCATE TABLE staging.veiculos RESTART IDENTITY CASCADE;
TRUNCATE TABLE staging.reservas RESTART IDENTITY CASCADE;
TRUNCATE TABLE staging.locacoes RESTART IDENTITY CASCADE;
TRUNCATE TABLE staging.cobrancas RESTART IDENTITY CASCADE;
TRUNCATE TABLE staging.acessorios RESTART IDENTITY CASCADE;
TRUNCATE TABLE staging.veiculos_acessorios RESTART IDENTITY CASCADE;
TRUNCATE TABLE staging.prontuarios_veiculos RESTART IDENTITY CASCADE;
TRUNCATE TABLE staging.fotos_veiculos RESTART IDENTITY CASCADE;


-- Recomenda-se a desativação de índices e constraints para melhor performance
-- durante a carga de grandes volumes de dados na staging, e reativação ao final.
-- Ex: ALTER TABLE staging.clientes DISABLE TRIGGER ALL;

-- 1. Tabela: staging.clientes
-- Inclui 'empresa_id' para identificar a origem dos dados
CREATE TABLE IF NOT EXISTS staging.clientes (
    cliente_id INTEGER,
    nome_completo VARCHAR(255),
    cpf_cnpj VARCHAR(18),
    tipo_pessoa CHAR(1),
    email VARCHAR(100),
    telefone VARCHAR(20),
    endereco_cidade VARCHAR(100),
    endereco_estado VARCHAR(50),
    data_cadastro TIMESTAMP WITHOUT TIME ZONE,
    empresa_id VARCHAR(50) -- Identificador da empresa de origem
);

-- Extrai dados da sua empresa (supondo empresa 'Aeroporto Galeão')
INSERT INTO staging.clientes (
    cliente_id, nome_completo, cpf_cnpj, tipo_pessoa, email, telefone,
    endereco_cidade, endereco_estado, data_cadastro, empresa_id
)
SELECT
    c.cliente_id, c.nome_completo, c.cpf_cnpj, c.tipo_pessoa, c.email, c.telefone,
    c.endereco_cidade, c.endereco_estado, c.data_cadastro, 'Empresa Galeao'
FROM public.clientes c;

-- Simula dados de outra empresa (ex: 'Santos Dumont')
INSERT INTO staging.clientes (
    cliente_id, nome_completo, cpf_cnpj, tipo_pessoa, email, telefone,
    endereco_cidade, endereco_estado, data_cadastro, empresa_id
) VALUES
(1001, 'João da Silva Santos Dumont', '111.111.111-11', 'F', 'joao.sd@email.com', '21987654321', 'Rio de Janeiro', 'RJ', '2023-01-15 10:00:00', 'Empresa Santos Dumont'),
(1002, 'Alpha Locadora SD', '11.222.333/0001-44', 'J', 'contato.alpha.sd@email.com', '2133334444', 'Niterói', 'RJ', '2023-02-20 11:30:00', 'Empresa Santos Dumont');

-- 2. Tabela: staging.motoristas
CREATE TABLE IF NOT EXISTS staging.motoristas (
    motorista_id INTEGER,
    cliente_id INTEGER,
    nome_completo VARCHAR(255),
    cnh VARCHAR(11),
    cnh_categoria VARCHAR(5),
    cnh_validade DATE,
    empresa_id VARCHAR(50)
);

INSERT INTO staging.motoristas (
    motorista_id, cliente_id, nome_completo, cnh, cnh_categoria, cnh_validade, empresa_id
)
SELECT
    m.motorista_id, m.cliente_id, m.nome_completo, m.cnh, m.cnh_categoria, m.cnh_validade, 'Empresa Galeao'
FROM public.motoristas m;

INSERT INTO staging.motoristas (
    motorista_id, cliente_id, nome_completo, cnh, cnh_categoria, cnh_validade, empresa_id
) VALUES
(2001, 1001, 'João Motorista SD', '98765432101', 'B', '2028-05-01', 'Empresa Santos Dumont'),
(2002, 1002, 'Maria Motorista SD', '12345678902', 'D', '2027-11-15', 'Empresa Santos Dumont');


-- 3. Tabela: staging.patios
CREATE TABLE IF NOT EXISTS staging.patios (
    patio_id INTEGER,
    nome VARCHAR(100),
    endereco VARCHAR(255),
    criado_em TIMESTAMP WITHOUT TIME ZONE,
    empresa_id VARCHAR(50)
);

INSERT INTO staging.patios (patio_id, nome, endereco, criado_em, empresa_id)
SELECT p.patio_id, p.nome, p.endereco, p.criado_em, 'Empresa Galeao'
FROM public.patios p;

INSERT INTO staging.patios (patio_id, nome, endereco, criado_em, empresa_id) VALUES
(10, 'Pátio Santos Dumont', 'Av. Santos Dumont, 900', '2020-01-01 09:00:00', 'Empresa Santos Dumont'),
(11, 'Pátio Rodoviária RJ', 'Rua Procópio Ferreira, 100', '2020-03-01 10:00:00', 'Empresa Rodoviaria');


-- 4. Tabela: staging.vagas
CREATE TABLE IF NOT EXISTS staging.vagas (
    vaga_id INTEGER,
    patio_id INTEGER,
    codigo VARCHAR(20),
    ocupada BOOLEAN,
    empresa_id VARCHAR(50)
);

INSERT INTO staging.vagas (vaga_id, patio_id, codigo, ocupada, empresa_id)
SELECT v.vaga_id, v.patio_id, v.codigo, v.ocupada, 'Empresa Galeao'
FROM public.vagas v;

INSERT INTO staging.vagas (vaga_id, patio_id, codigo, ocupada, empresa_id) VALUES
(100, 10, 'SD-A01', FALSE, 'Empresa Santos Dumont'),
(101, 10, 'SD-A02', TRUE, 'Empresa Santos Dumont'),
(200, 11, 'RD-B01', FALSE, 'Empresa Rodoviaria');


-- 5. Tabela: staging.grupos_veiculos
CREATE TABLE IF NOT EXISTS staging.grupos_veiculos (
    grupo_id INTEGER,
    nome_grupo VARCHAR(50),
    descricao_grupo TEXT,
    tarifa_diaria_base DECIMAL(10,2),
    empresa_id VARCHAR(50)
);

INSERT INTO staging.grupos_veiculos (grupo_id, nome_grupo, descricao_grupo, tarifa_diaria_base, empresa_id)
SELECT gv.grupo_id, gv.nome_grupo, gv.descricao_grupo, gv.tarifa_diaria_base, 'Empresa Galeao'
FROM public.grupos_veiculos gv;

INSERT INTO staging.grupos_veiculos (grupo_id, nome_grupo, descricao_grupo, tarifa_diaria_base, empresa_id) VALUES
(10, 'Compacto SD', 'Veículos compactos e econômicos para a cidade SD', 80.00, 'Empresa Santos Dumont'),
(11, 'Luxo RD', 'Veículos de luxo para a rodoviária', 500.00, 'Empresa Rodoviaria');


-- 6. Tabela: staging.veiculos
CREATE TABLE IF NOT EXISTS staging.veiculos (
    veiculo_id INTEGER,
    placa VARCHAR(10),
    chassi VARCHAR(17),
    grupo_id INTEGER,
    vaga_atual_id INTEGER,
    marca VARCHAR(50),
    modelo VARCHAR(50),
    cor VARCHAR(30),
    ano_fabricacao INTEGER,
    cambio VARCHAR(20),
    possui_ar_cond BOOLEAN,
    situacao VARCHAR(20),
    empresa_id VARCHAR(50)
);

INSERT INTO staging.veiculos (
    veiculo_id, placa, chassi, grupo_id, vaga_atual_id, marca, modelo, cor,
    ano_fabricacao, cambio, possui_ar_cond, situacao, empresa_id
)
SELECT
    v.veiculo_id, v.placa, v.chassi, v.grupo_id, v.vaga_atual_id, v.marca, v.modelo, v.cor,
    v.ano_fabricacao, v.cambio, v.possui_ar_cond, v.situacao, 'Empresa Galeao'
FROM public.veiculos v;

INSERT INTO staging.veiculos (
    veiculo_id, placa, chassi, grupo_id, vaga_atual_id, marca, modelo, cor,
    ano_fabricacao, cambio, possui_ar_cond, situacao, empresa_id
) VALUES
(1001, 'ABC1D23', 'CHASSISD1001', 10, 101, 'Fiat', 'Mobi', 'Branco', 2022, 'Manual', TRUE, 'Alugado', 'Empresa Santos Dumont'),
(1002, 'XYZ9W87', 'CHASSISD1002', 10, NULL, 'Hyundai', 'HB20', 'Prata', 2023, 'Automática', TRUE, 'Disponível', 'Empresa Santos Dumont'),
(2001, 'DEF4G56', 'CHASSIRD2001', 11, NULL, 'Mercedes', 'C180', 'Preto', 2024, 'Automática', TRUE, 'Disponível', 'Empresa Rodoviaria');


-- 7. Tabela: staging.reservas
CREATE TABLE IF NOT EXISTS staging.reservas (
    reserva_id INTEGER,
    cliente_id INTEGER,
    grupo_id INTEGER,
    patio_retirada_id INTEGER,
    criado_em TIMESTAMP WITHOUT TIME ZONE,
    retirada_prevista TIMESTAMP WITHOUT TIME ZONE,
    devolucao_prevista TIMESTAMP WITHOUT TIME ZONE,
    situacao_reserva VARCHAR(20),
    empresa_id VARCHAR(50)
);

INSERT INTO staging.reservas (
    reserva_id, cliente_id, grupo_id, patio_retirada_id, criado_em,
    retirada_prevista, devolucao_prevista, situacao_reserva, empresa_id
)
SELECT
    r.reserva_id, r.cliente_id, r.grupo_id, r.patio_retirada_id, r.criado_em,
    r.retirada_prevista, r.devolucao_prevista, r.situacao_reserva, 'Empresa Galeao'
FROM public.reservas r;

INSERT INTO staging.reservas (
    reserva_id, cliente_id, grupo_id, patio_retirada_id, criado_em,
    retirada_prevista, devolucao_prevista, situacao_reserva, empresa_id
) VALUES
(3001, 1001, 10, 10, '2024-06-01 10:00:00', '2024-07-01 09:00:00', '2024-07-05 09:00:00', 'Ativa', 'Empresa Santos Dumont'),
(3002, 1002, 10, 10, '2024-06-05 14:00:00', '2024-07-10 10:00:00', '2024-07-12 10:00:00', 'Ativa', 'Empresa Santos Dumont'),
(4001, 1001, 11, 11, '2024-06-10 11:00:00', '2024-08-01 15:00:00', '2024-08-03 15:00:00', 'Ativa', 'Empresa Rodoviaria');


-- 8. Tabela: staging.locacoes
CREATE TABLE IF NOT EXISTS staging.locacoes (
    locacao_id INTEGER,
    reserva_id INTEGER,
    cliente_id INTEGER,
    motorista_id INTEGER,
    veiculo_id INTEGER,
    patio_retirada_id INTEGER,
    patio_devolucao_id INTEGER,
    retirada_real TIMESTAMP WITHOUT TIME ZONE,
    devolucao_prevista TIMESTAMP WITHOUT TIME ZONE,
    devolucao_real TIMESTAMP WITHOUT TIME ZONE,
    valor_previsto DECIMAL(10,2),
    valor_final DECIMAL(10,2),
    protecoes_extras TEXT,
    empresa_id VARCHAR(50)
);

INSERT INTO staging.locacoes (
    locacao_id, reserva_id, cliente_id, motorista_id, veiculo_id,
    patio_retirada_id, patio_devolucao_id, retirada_real, devolucao_prevista,
    devolucao_real, valor_previsto, valor_final, protecoes_extras, empresa_id
)
SELECT
    l.locacao_id, l.reserva_id, l.cliente_id, l.motorista_id, l.veiculo_id,
    l.patio_retirada_id, l.patio_devolucao_id, l.retirada_real, l.devolucao_prevista,
    l.devolucao_real, l.valor_previsto, l.valor_final, l.protecoes_extras, 'Empresa Galeao'
FROM public.locacoes l;

INSERT INTO staging.locacoes (
    locacao_id, reserva_id, cliente_id, motorista_id, veiculo_id,
    patio_retirada_id, patio_devolucao_id, retirada_real, devolucao_prevista,
    devolucao_real, valor_previsto, valor_final, protecoes_extras, empresa_id
) VALUES
-- Locação da Empresa Santos Dumont
(3001, NULL, 1001, 2001, 1001, 10, 10, '2024-06-15 09:00:00', '2024-06-17 09:00:00', '2024-06-17 08:50:00', 160.00, 160.00, 'Nenhum', 'Empresa Santos Dumont'),
(3002, NULL, 1001, 2001, 1002, 10, 11, '2024-06-18 10:00:00', '2024-06-20 10:00:00', NULL, 160.00, NULL, 'Seguro Completo', 'Empresa Santos Dumont'), -- Locação em andamento, devolvida em outro patio
(4001, NULL, 1002, 2002, 2001, 11, 10, '2024-06-19 11:00:00', '2024-06-21 11:00:00', '2024-06-21 11:30:00', 1000.00, 1050.00, 'Cobertura Vidros', 'Empresa Rodoviaria'); -- Locação da Empresa Rodoviaria


-- 9. Tabela: staging.cobrancas
CREATE TABLE IF NOT EXISTS staging.cobrancas (
    cobranca_id INTEGER,
    locacao_id INTEGER,
    valor DECIMAL(10,2),
    emitida_em TIMESTAMP WITHOUT TIME ZONE,
    vencimento DATE,
    pago_em DATE,
    status_pago VARCHAR(20),
    empresa_id VARCHAR(50)
);

INSERT INTO staging.cobrancas (
    cobranca_id, locacao_id, valor, emitida_em, vencimento, pago_em, status_pago, empresa_id
)
SELECT
    c.cobranca_id, c.locacao_id, c.valor, c.emitida_em, c.vencimento, c.pago_em, c.status_pago, 'Empresa Galeao'
FROM public.cobrancas c;

INSERT INTO staging.cobrancas (
    cobranca_id, locacao_id, valor, emitida_em, vencimento, pago_em, status_pago, empresa_id
) VALUES
(3001, 3001, 160.00, '2024-06-17 09:00:00', '2024-06-17', '2024-06-17', 'Pago', 'Empresa Santos Dumont'),
(3002, 3002, 160.00, '2024-06-18 10:00:00', '2024-06-20', '2024-06-18', 'Pago', 'Empresa Santos Dumont'), -- Pagamento antecipado
(4001, 4001, 1050.00, '2024-06-21 11:30:00', '2024-06-21', '2024-06-21', 'Pago', 'Empresa Rodoviaria');


-- 10. Tabela: staging.acessorios
CREATE TABLE IF NOT EXISTS staging.acessorios (
    acessorio_id INTEGER,
    nome_acessorio VARCHAR(100),
    descricao_acessorio TEXT,
    empresa_id VARCHAR(50)
);

INSERT INTO staging.acessorios (acessorio_id, nome_acessorio, descricao_acessorio, empresa_id)
SELECT a.acessorio_id, a.nome_acessorio, a.descricao_acessorio, 'Empresa Galeao'
FROM public.acessorios a;

INSERT INTO staging.acessorios (acessorio_id, nome_acessorio, descricao_acessorio, empresa_id) VALUES
(100, 'Cadeirinha Bebê SD', 'Cadeirinha infantil para bebês SD', 'Empresa Santos Dumont'),
(101, 'GPS RD', 'Sistema de navegação GPS avançado RD', 'Empresa Rodoviaria');


-- 11. Tabela: staging.veiculos_acessorios
CREATE TABLE IF NOT EXISTS staging.veiculos_acessorios (
    veiculo_id INTEGER,
    acessorio_id INTEGER,
    empresa_id VARCHAR(50)
);

INSERT INTO staging.veiculos_acessorios (veiculo_id, acessorio_id, empresa_id)
SELECT va.veiculo_id, va.acessorio_id, 'Empresa Galeao'
FROM public.veiculos_acessorios va;

INSERT INTO staging.veiculos_acessorios (veiculo_id, acessorio_id, empresa_id) VALUES
(1001, 100, 'Empresa Santos Dumont'),
(2001, 101, 'Empresa Rodoviaria');


-- 12. Tabela: staging.prontuarios_veiculos
CREATE TABLE IF NOT EXISTS staging.prontuarios_veiculos (
    prontuario_id INTEGER,
    veiculo_id INTEGER,
    data_evento DATE,
    tipo_evento VARCHAR(50),
    detalhes TEXT,
    custo_evento DECIMAL(10,2),
    empresa_id VARCHAR(50)
);

INSERT INTO staging.prontuarios_veiculos (
    prontuario_id, veiculo_id, data_evento, tipo_evento, detalhes, custo_evento, empresa_id
)
SELECT
    pv.prontuario_id, pv.veiculo_id, pv.data_evento, pv.tipo_evento, pv.detalhes, pv.custo_evento, 'Empresa Galeao'
FROM public.prontuarios_veiculos pv;

INSERT INTO staging.prontuarios_veiculos (
    prontuario_id, veiculo_id, data_evento, tipo_evento, detalhes, custo_evento, empresa_id
) VALUES
(1001, 1001, '2024-05-01', 'Manutenção Preventiva', 'Troca de óleo e filtros', 150.00, 'Empresa Santos Dumont'),
(1002, 1002, '2024-06-10', 'Revisão', 'Revisão geral antes de nova locação', 200.00, 'Empresa Santos Dumont'),
(2001, 2001, '2024-06-15', 'Avaria', 'Pequeno arranhão na lateral direita', 300.00, 'Empresa Rodoviaria');


-- 13. Tabela: staging.fotos_veiculos
CREATE TABLE IF NOT EXISTS staging.fotos_veiculos (
    foto_id INTEGER,
    veiculo_id INTEGER,
    caminho_imagem VARCHAR(255),
    finalidade VARCHAR(50),
    enviado_em TIMESTAMP WITHOUT TIME ZONE,
    empresa_id VARCHAR(50)
);

INSERT INTO staging.fotos_veiculos (
    foto_id, veiculo_id, caminho_imagem, finalidade, enviado_em, empresa_id
)
SELECT
    fv.foto_id, fv.veiculo_id, fv.caminho_imagem, fv.finalidade, fv.enviado_em, 'Empresa Galeao'
FROM public.fotos_veiculos fv;

INSERT INTO staging.fotos_veiculos (
    foto_id, veiculo_id, caminho_imagem, finalidade, enviado_em, empresa_id
) VALUES
(1001, 1001, 'http://fotosd.com/mobi1.jpg', 'Propaganda', '2024-04-20 10:00:00', 'Empresa Santos Dumont'),
(1002, 1001, 'http://fotosd.com/mobi_entrega.jpg', 'Entrega', '2024-06-15 08:30:00', 'Empresa Santos Dumont'),
(2001, 2001, 'http://fotosr.com/mercedes_avaria.jpg', 'Avaria', '2024-06-19 11:15:00', 'Empresa Rodoviaria');
