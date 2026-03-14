ALTER TABLE sale_agg_1min ADD COLUMN pressione2_avg DECIMAL(18, 4) NULL AFTER pressione_avg;
ALTER TABLE sale_agg_1min ADD COLUMN temperatura2_avg DECIMAL(18, 4) NULL AFTER temperatura_avg;

ALTER TABLE sale_agg_15min ADD COLUMN pressione2_avg DECIMAL(18, 4) NULL AFTER pressione_avg;
ALTER TABLE sale_agg_15min ADD COLUMN temperatura2_avg DECIMAL(18, 4) NULL AFTER temperatura_avg;

ALTER TABLE sale_agg_1h ADD COLUMN pressione2_avg DECIMAL(18, 4) NULL AFTER pressione_avg;
ALTER TABLE sale_agg_1h ADD COLUMN temperatura2_avg DECIMAL(18, 4) NULL AFTER temperatura_avg;

ALTER TABLE sale_agg_1d ADD COLUMN pressione2_avg DECIMAL(18, 4) NULL AFTER pressione_avg;
ALTER TABLE sale_agg_1d ADD COLUMN temperatura2_avg DECIMAL(18, 4) NULL AFTER temperatura_avg;

ALTER TABLE sale_agg_1month ADD COLUMN pressione2_avg DECIMAL(18, 4) NULL AFTER pressione_avg;
ALTER TABLE sale_agg_1month ADD COLUMN temperatura2_avg DECIMAL(18, 4) NULL AFTER temperatura_avg;
