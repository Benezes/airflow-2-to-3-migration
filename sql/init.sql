CREATE SCHEMA IF NOT EXISTS br_market;

CREATE TABLE IF NOT EXISTS br_market.cvm_fundos (
    cnpj CHAR(14) NOT NULL,
    denom_social TEXT NOT NULL,
    classe TEXT,
    sit TEXT,
    dt_reg DATE,
    vl_patrim_liq NUMERIC(18, 2),
    ref_date DATE NOT NULL,
    PRIMARY KEY (cnpj, ref_date)
);

CREATE TABLE IF NOT EXISTS br_market.b3_indicators (
    code TEXT NOT NULL,
    ref_date DATE NOT NULL,
    close NUMERIC(18, 2),
    prev_close NUMERIC(18, 2),
    daily_return NUMERIC(18, 8),
    PRIMARY KEY (code, ref_date)
);

CREATE TABLE IF NOT EXISTS br_market.anbima_ettj (
    ref_date DATE NOT NULL,
    curva TEXT NOT NULL,
    vertice_dias INTEGER NOT NULL,
    taxa NUMERIC(18, 8),
    PRIMARY KEY (ref_date, curva, vertice_dias)
);

CREATE TABLE IF NOT EXISTS br_market.bcb_rates_staging (
    ref_date DATE NOT NULL,
    series_id TEXT NOT NULL,
    rate_pct NUMERIC(18, 2),
    PRIMARY KEY (ref_date, series_id)
);

CREATE TABLE IF NOT EXISTS br_market.bcb_rates_mart (
    ref_date DATE NOT NULL,
    series_id TEXT NOT NULL,
    rate_pct NUMERIC(18, 2),
    source_system TEXT NOT NULL,
    PRIMARY KEY (ref_date, series_id)
);

CREATE TABLE IF NOT EXISTS br_market.proventos_eventos (
    ticker TEXT NOT NULL,
    tipo TEXT NOT NULL,
    data_ex DATE NOT NULL,
    data_pagamento DATE,
    valor_por_cota NUMERIC(18, 2),
    PRIMARY KEY (ticker, data_ex, tipo)
);

CREATE TABLE IF NOT EXISTS br_market.proventos_aplicados (
    ticker TEXT NOT NULL,
    tipo TEXT NOT NULL,
    ref_date DATE NOT NULL,
    data_ex DATE NOT NULL,
    data_pagamento DATE,
    cotas NUMERIC(18, 2),
    valor_por_cota NUMERIC(18, 2),
    gross_amount NUMERIC(18, 2),
    ir_amount NUMERIC(18, 2),
    net_amount NUMERIC(18, 2),
    PRIMARY KEY (ticker, data_ex, tipo)
);
