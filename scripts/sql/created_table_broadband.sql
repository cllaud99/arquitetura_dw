CREATE TABLE IF NOT EXISTS raw_broadband_copy (
    ano SMALLINT,
    mes SMALLINT,
    grupo_economico VARCHAR(30),
    empresa VARCHAR(125),
    cnpj CHAR(14),
    porte_da_prestadora VARCHAR(15),
    uf CHAR(2),
    municipio VARCHAR(35),
    codigo_ibge_municipio INTEGER,
    faixa_de_velocidade VARCHAR(15),
    velocidade DOUBLE PRECISION,
    tecnologia VARCHAR(15),  -- Corrigido de "vecnologia"
    meio_de_acesso VARCHAR(15),
    tipo_de_pessoa VARCHAR(15),
    tipo_de_produto VARCHAR(15),
    acessos INTEGER
);


CREATE TABLE IF NOT EXISTS raw_smp_copy (
    periodo VARCHAR(10),
    operadora VARCHAR(5),
    municipio VARCHAR(35),
    uf CHAR(2),
    codigo_ibge INTEGER,
    tecnologia CHAR(2),
    presenca VARCHAR(4)
);