-- Create tables for analytics-camara application

-- Deputies table
CREATE TABLE deputados (
    id INTEGER PRIMARY KEY,
    uri VARCHAR NOT NULL,
    nome_civil VARCHAR NOT NULL,
    cpf VARCHAR,
    sexo VARCHAR,
    url_website VARCHAR,
    data_nascimento DATE,
    data_falecimento DATE,
    uf_nascimento VARCHAR,
    municipio_nascimento VARCHAR,
    escolaridade VARCHAR,
    ultimo_status_id INTEGER,
    ultimo_status_nome VARCHAR,
    ultimo_status_sigla_partido VARCHAR,
    ultimo_status_uri_partido VARCHAR,
    ultimo_status_sigla_uf VARCHAR,
    ultimo_status_id_legislatura INTEGER,
    ultimo_status_url_foto VARCHAR,
    ultimo_status_email VARCHAR,
    ultimo_status_data DATE,
    ultimo_status_nome_eleitoral VARCHAR,
    ultimo_status_gabinete JSONB,
    ultimo_status_situacao VARCHAR,
    ultimo_status_condicao_eleitoral VARCHAR,
    ultimo_status_descricao VARCHAR
);

-- Expenses table
CREATE TABLE despesas (
    id SERIAL PRIMARY KEY,
    deputado_id INTEGER NOT NULL REFERENCES deputados(id),
    ano INTEGER NOT NULL,
    mes INTEGER NOT NULL,
    tipo_despesa VARCHAR NOT NULL,
    cod_documento INTEGER NOT NULL,
    tipo_documento VARCHAR NOT NULL,
    cod_tipo_documento INTEGER NOT NULL,
    data_documento DATE NOT NULL,
    num_documento VARCHAR NOT NULL,
    valor_documento NUMERIC NOT NULL,
    url_documento VARCHAR NOT NULL,
    nome_fornecedor VARCHAR NOT NULL,
    cnpj_cpf_fornecedor VARCHAR NOT NULL,
    valor_liquido NUMERIC NOT NULL,
    valor_glosa NUMERIC NOT NULL,
    num_ressarcimento VARCHAR,
    cod_lote INTEGER,
    parcela INTEGER
);

-- Speeches table
CREATE TABLE discursos (
    id SERIAL PRIMARY KEY,
    deputado_id INTEGER NOT NULL REFERENCES deputados(id),
    data_hora_inicio TIMESTAMP NOT NULL,
    data_hora_fim TIMESTAMP,
    fase_evento JSONB,
    tipo_discurso VARCHAR NOT NULL,
    url_texto VARCHAR,
    url_audio VARCHAR,
    url_video VARCHAR,
    keywords VARCHAR,
    sumario TEXT,
    transcricao TEXT
);

-- Voting sessions table
CREATE TABLE votacoes (
    id VARCHAR PRIMARY KEY,
    uri VARCHAR NOT NULL,
    data DATE NOT NULL,
    data_hora_registro TIMESTAMP NOT NULL,
    sigla_orgao VARCHAR NOT NULL,
    uri_orgao VARCHAR NOT NULL,
    proposicao_objeto JSONB,
    tipo_votacao JSONB NOT NULL,
    ultima_apresentacao_proposicao JSONB,
    aprovacao BOOLEAN NOT NULL
);

-- Individual votes table
CREATE TABLE votos (
    id SERIAL PRIMARY KEY,
    votacao_id VARCHAR NOT NULL REFERENCES votacoes(id),
    deputado_id INTEGER NOT NULL REFERENCES deputados(id),
    data_registro_voto TIMESTAMP NOT NULL,
    tipo_voto VARCHAR NOT NULL
);

-- Create indexes for foreign keys to improve query performance
CREATE INDEX idx_despesas_deputado_id ON despesas(deputado_id);
CREATE INDEX idx_discursos_deputado_id ON discursos(deputado_id);
CREATE INDEX idx_votos_deputado_id ON votos(deputado_id);
CREATE INDEX idx_votos_votacao_id ON votos(votacao_id);
