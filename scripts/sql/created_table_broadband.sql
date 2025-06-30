CREATE TABLE IF NOT EXISTS raw_broadband (
    "ano" INTEGER,
    "mês" INTEGER,
    "grupo Econômico" VARCHAR,
    "empresa" VARCHAR,
    "cnpj" CHAR(14),
    "porte da prestadora" VARCHAR,
    "uf" CHAR(2),
    "município" VARCHAR,
    "código ibge município" INTEGER,
    "faixa de velocidade" VARCHAR,
    "velocidade" DOUBLE PRECISION,
    "vecnologia" VARCHAR,
    "meio de acesso" VARCHAR,
    "tipo de pessoa" VARCHAR,
    "tipo de produto" VARCHAR,
    "acessos" INTEGER
);
