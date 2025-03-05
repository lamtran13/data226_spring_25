CREATE DATABASE IF NOT EXISTS dev;
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS analytics;
CREATE TABLE AMZN_price;

CREATE OR REPLACE SNOWFLAKE.ML.FORECAST dev.analytics.predict_tsla_price (
    INPUT_DATA => SYSTEM$REFERENCE('VIEW', 'dev.adhoc.tsla_price_view'),
    SERIES_COLNAME => 'SYMBOL',
    TIMESTAMP_COLNAME => 'DATE',
    TARGET_COLNAME => 'CLOSE',
    CONFIG_OBJECT => { 'ON_ERROR': 'SKIP' }
);

CALL dev.analytics.predict_tsla_price!FORECAST(
    FORECASTING_PERIODS => 7,
    CONFIG_OBJECT => { 'prediction_interval': 0.95 }
);
