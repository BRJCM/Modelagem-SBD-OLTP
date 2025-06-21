-- Avaliação 02: Modelagem de Data Warehouse - Parte 2
-- Brian José Costa de Medeiro DRE: 121087678
-- Esquema Estrela do Data Warehouse

-- Criação do esquema para o Data Warehouse
CREATE SCHEMA IF NOT EXISTS dw;

-- Dimensão de Tempo (Dim_Tempo)
-- Finalidade: Fornecer atributos temporais para análise de fatos.
-- Justificativa dos campos: Detalham a data em diversas granularidades (ano, mês, dia, etc.)
-- para flexibilidade nas consultas e relatórios.
CREATE TABLE dw.Dim_Tempo (
    sk_tempo               SERIAL PRIMARY KEY,
    data_completa          DATE NOT NULL UNIQUE,
    ano                    SMALLINT NOT NULL,
    trimestre              SMALLINT NOT NULL,
    mes                    SMALLINT NOT NULL,
    nome_mes               VARCHAR(20) NOT NULL,
    dia_do_mes             SMALLINT NOT NULL,
    dia_da_semana          SMALLINT NOT NULL,
    nome_dia_da_semana     VARCHAR(20) NOT NULL,
    semana_do_ano          SMALLINT NOT NULL,
    feriado_br             BOOLEAN DEFAULT FALSE, -- Exemplo: para marcar feriados
    eh_fim_de_semana       BOOLEAN NOT NULL,
    hora_completa          TIME, -- Para granularidade horária, se necessária para algum fato
    hora                   SMALLINT,
    minuto                 SMALLINT
);

-- Dimensão de Cliente (Dim_Cliente)
-- Finalidade: Armazenar informações sobre os contratantes das locações/reservas.
-- Justificativa dos campos: 'sk_cliente' é a chave substituta. 'cliente_id_oltp'
-- é a chave natural para rastreamento. Demais campos são atributos descritivos.
CREATE TABLE dw.Dim_Cliente (
    sk_cliente             SERIAL PRIMARY KEY,
    cliente_id_oltp        INTEGER NOT NULL, -- Chave natural do sistema OLTP
    nome_completo          VARCHAR(255) NOT NULL,
    cpf_cnpj               VARCHAR(18),
    tipo_pessoa            CHAR(1) NOT NULL, -- 'F' para Pessoa Física, 'J' para Pessoa Jurídica
    email                  VARCHAR(100),
    telefone               VARCHAR(20),
    cidade_cliente         VARCHAR(100),
    estado_cliente         VARCHAR(50),
    data_cadastro          TIMESTAMP WITHOUT TIME ZONE,
    -- Campos de controle de SCD (Type 2) podem ser adicionados aqui se necessário
    -- data_inicio_validade DATE DEFAULT CURRENT_DATE,
    -- data_fim_validade    DATE,
    -- versao               INTEGER
    UNIQUE (cliente_id_oltp, tipo_pessoa) -- Para garantir unicidade de cliente, considerando se CPF/CNPJ pode ser nulo para alguns registros
);

-- Dimensão de Motorista (Dim_Motorista)
-- Finalidade: Listar os condutores habilitados associados a clientes e locações.
-- Justificativa dos campos: 'sk_motorista' é a chave substituta. 'motorista_id_oltp'
-- é a chave natural. 'sk_cliente_responsavel' é uma chave estrangeira para
-- Dim_Cliente, conectando o motorista ao cliente que o autoriza (especialmente PJ).
CREATE TABLE dw.Dim_Motorista (
    sk_motorista              SERIAL PRIMARY KEY,
    motorista_id_oltp         INTEGER NOT NULL, -- Chave natural do sistema OLTP
    sk_cliente_responsavel    INTEGER NOT NULL REFERENCES dw.Dim_Cliente(sk_cliente),
    nome_completo_motorista   VARCHAR(255) NOT NULL,
    cnh                       VARCHAR(11) NOT NULL,
    cnh_categoria             VARCHAR(5) NOT NULL,
    cnh_validade              DATE NOT NULL,
    UNIQUE (motorista_id_oltp)
);

-- Dimensão de Pátio (Dim_Patio)
-- Finalidade: Descrever os locais físicos de retirada e devolução de veículos.
-- Justificativa dos campos: 'sk_patio' é a chave substituta. 'patio_id_oltp'
-- é a chave natural. Demais campos são descritivos do local.
CREATE TABLE dw.Dim_Patio (
    sk_patio               SERIAL PRIMARY KEY,
    patio_id_oltp          INTEGER NOT NULL, -- Chave natural do sistema OLTP
    nome_patio             VARCHAR(100) NOT NULL UNIQUE,
    endereco_patio         VARCHAR(255) NOT NULL,
    cidade_patio           VARCHAR(100), -- Derivado do endereço
    estado_patio           VARCHAR(50),  -- Derivado do endereço
    UNIQUE (patio_id_oltp)
);

-- Dimensão de Veículo (Dim_Veiculo)
-- Finalidade: Detalhar os veículos da frota e seus respectivos grupos.
-- Justificativa dos campos: 'sk_veiculo' é a chave substituta. 'veiculo_id_oltp'
-- é a chave natural. Inclui atributos do veículo (placa, marca, modelo) e
-- atributos do grupo de veículo (nome_grupo, tarifa_diaria_base),
-- conformados em uma única dimensão. 'origem_frota' é crucial para o relatório
-- de controle de pátio, indicando se o veículo é da empresa "própria" ou "externa".
CREATE TABLE dw.Dim_Veiculo (
    sk_veiculo               SERIAL PRIMARY KEY,
    veiculo_id_oltp          INTEGER NOT NULL, -- Chave natural do sistema OLTP
    placa                    VARCHAR(10) NOT NULL,
    chassi                   VARCHAR(17) NOT NULL,
    marca                    VARCHAR(50) NOT NULL,
    modelo                   VARCHAR(50) NOT NULL,
    cor                      VARCHAR(30) NOT NULL,
    ano_fabricacao           INTEGER NOT NULL,
    cambio                   VARCHAR(20) NOT NULL,
    possui_ar_cond           BOOLEAN NOT NULL,
    situacao_atual_oltp      VARCHAR(20) NOT NULL, -- Status atual no OLTP
    nome_grupo_veiculo       VARCHAR(50) NOT NULL,
    descricao_grupo_veiculo  TEXT,
    tarifa_diaria_base_grupo DECIMAL(10,2) NOT NULL,
    origem_frota             VARCHAR(50) NOT NULL, -- 'Propria' ou 'Externa'
    UNIQUE (veiculo_id_oltp)
);

-- Fato de Locação (Fato_Locacao)
-- Finalidade: Registrar as locações efetivadas para análises de receita, duração
-- e fluxo de pátios.
-- Granularidade: Uma linha por cada locação.
-- Justificativa dos campos: Contém chaves estrangeiras para as dimensões
-- que contextualizam a locação (tempo, cliente, motorista, veículo, pátios) e
-- medidas que quantificam o evento (valores, durações). 'locacao_id_oltp' e
-- 'reserva_id_oltp' são dimensões degeneradas para rastreabilidade.
CREATE TABLE dw.Fato_Locacao (
    sk_locacao                  SERIAL PRIMARY KEY,
    sk_tempo_retirada           INTEGER NOT NULL REFERENCES dw.Dim_Tempo(sk_tempo),
    sk_tempo_devolucao_prevista INTEGER NOT NULL REFERENCES dw.Dim_Tempo(sk_tempo),
    sk_tempo_devolucao_real     INTEGER REFERENCES dw.Dim_Tempo(sk_tempo), -- Pode ser NULL
    sk_cliente                  INTEGER NOT NULL REFERENCES dw.Dim_Cliente(sk_cliente),
    sk_motorista                INTEGER NOT NULL REFERENCES dw.Dim_Motorista(sk_motorista),
    sk_veiculo                  INTEGER NOT NULL REFERENCES dw.Dim_Veiculo(sk_veiculo),
    sk_patio_retirada           INTEGER NOT NULL REFERENCES dw.Dim_Patio(sk_patio),
    sk_patio_devolucao          INTEGER REFERENCES dw.Dim_Patio(sk_patio), -- Pode ser NULL

    locacao_id_oltp             INTEGER NOT NULL, -- Dimensão Degenerada
    reserva_id_oltp             INTEGER,          -- Dimensão Degenerada (NULL se walk-in)

    valor_previsto              DECIMAL(10,2) NOT NULL,
    valor_final                 DECIMAL(10,2),    -- Pode ser NULL
    duracao_em_dias_prevista    DECIMAL(10,2) NOT NULL,
    duracao_em_dias_real        DECIMAL(10,2),    -- Pode ser NULL
    dias_restantes_para_devolucao DECIMAL(10,2), -- Medida calculada para locações ativas
    CONSTRAINT fk_sk_tempo_retirada FOREIGN KEY (sk_tempo_retirada) REFERENCES dw.Dim_Tempo(sk_tempo),
    CONSTRAINT fk_sk_tempo_devolucao_prevista FOREIGN KEY (sk_tempo_devolucao_prevista) REFERENCES dw.Dim_Tempo(sk_tempo),
    CONSTRAINT fk_sk_tempo_devolucao_real FOREIGN KEY (sk_tempo_devolucao_real) REFERENCES dw.Dim_Tempo(sk_tempo)
);

-- Fato de Reserva (Fato_Reserva)
-- Finalidade: Acompanhar o volume de reservas, preferências de veículos e pátios,
-- e a antecedência das reservas.
-- Granularidade: Uma linha por cada reserva.
-- Justificativa dos campos: Contém chaves estrangeiras para as dimensões
-- (tempo, cliente, veículo, pátio) e medidas que quantificam a reserva.
-- 'reserva_id_oltp' e 'situacao_reserva_oltp' são dimensões degeneradas.
CREATE TABLE dw.Fato_Reserva (
    sk_reserva                  SERIAL PRIMARY KEY,
    sk_tempo_criacao_reserva    INTEGER NOT NULL REFERENCES dw.Dim_Tempo(sk_tempo),
    sk_tempo_retirada_prevista  INTEGER NOT NULL REFERENCES dw.Dim_Tempo(sk_tempo),
    sk_tempo_devolucao_prevista INTEGER NOT NULL REFERENCES dw.Dim_Tempo(sk_tempo),
    sk_cliente                  INTEGER NOT NULL REFERENCES dw.Dim_Cliente(sk_cliente),
    sk_veiculo_grupo            INTEGER NOT NULL REFERENCES dw.Dim_Veiculo(sk_veiculo), -- Ligação ao grupo do veículo
    sk_patio_retirada           INTEGER NOT NULL REFERENCES dw.Dim_Patio(sk_patio),

    reserva_id_oltp             INTEGER NOT NULL, -- Dimensão Degenerada
    situacao_reserva_oltp       VARCHAR(20) NOT NULL, -- Dimensão Degenerada

    quantidade_reservas         INTEGER NOT NULL DEFAULT 1,
    duracao_em_dias_prevista    DECIMAL(10,2) NOT NULL,
    dias_ate_retirada_prevista  DECIMAL(10,2) NOT NULL, -- Medida calculada
    CONSTRAINT fk_sk_tempo_criacao_reserva FOREIGN KEY (sk_tempo_criacao_reserva) REFERENCES dw.Dim_Tempo(sk_tempo),
    CONSTRAINT fk_sk_tempo_retirada_prevista FOREIGN KEY (sk_tempo_retirada_prevista) REFERENCES dw.Dim_Tempo(sk_tempo),
    CONSTRAINT fk_sk_tempo_devolucao_prevista_reserva FOREIGN KEY (sk_tempo_devolucao_prevista) REFERENCES dw.Dim_Tempo(sk_tempo)
);

-- Fato de Movimentação de Pátio (Fato_Movimentacao_Patio)
-- Finalidade: Registrar os movimentos de veículos entre pátios (retirada e devolução)
-- para a análise de cadeia de Markov e controle de ocupação.
-- Granularidade: Uma linha por cada devolução de veículo.
-- Justificativa dos campos: Captura o veículo, o pátio de origem (retirada) e destino
-- (devolução) e o tempo da devolução. 'locacao_id_oltp' é degenerada para rastreamento.
CREATE TABLE dw.Fato_Movimentacao_Patio (
    sk_movimentacao_patio    SERIAL PRIMARY KEY,
    sk_tempo_devolucao       INTEGER NOT NULL REFERENCES dw.Dim_Tempo(sk_tempo),
    sk_veiculo               INTEGER NOT NULL REFERENCES dw.Dim_Veiculo(sk_veiculo),
    sk_patio_origem          INTEGER NOT NULL REFERENCES dw.Dim_Patio(sk_patio),     -- Pátio de retirada
    sk_patio_destino         INTEGER NOT NULL REFERENCES dw.Dim_Patio(sk_patio),     -- Pátio de devolução

    locacao_id_oltp          INTEGER NOT NULL, -- Dimensão Degenerada
    quantidade_movimentos    INTEGER NOT NULL DEFAULT 1, -- Medida para contagem
    CONSTRAINT fk_sk_tempo_devolucao_mov FOREIGN KEY (sk_tempo_devolucao) REFERENCES dw.Dim_Tempo(sk_tempo)
);

