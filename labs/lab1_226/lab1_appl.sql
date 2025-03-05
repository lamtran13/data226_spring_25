CREATE DATABASE IF NOT EXISTS dev;
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS analytics;
CREATE SCHEMA IF NOT EXISTS dev.adhoc;


SELECT * FROM DEV.RAW.AAPL_PRICE; --Check the apple_price table
SELECT CURRENT_ACCOUNT() --Check the account number 


-- create the view for training
CREATE OR REPLACE VIEW dev.adhoc.aapl_price_view AS 
SELECT DATE, CLOSE, SYMBOL 
FROM dev.raw.aapl_price;

-- Use for Forcasting 
CREATE OR REPLACE SNOWFLAKE.ML.FORECAST dev.analytics.predict_tsla_price (
    INPUT_DATA => SYSTEM$REFERENCE('VIEW', 'dev.adhoc.aapl_price_view'),
    SERIES_COLNAME => 'SYMBOL',
    TIMESTAMP_COLNAME => 'DATE',
    TARGET_COLNAME => 'CLOSE',
    CONFIG_OBJECT => { 'ON_ERROR': 'SKIP' }
);

CALL dev.analytics.predict_appl_price!FORECAST(
    FORECASTING_PERIODS => 7,
    CONFIG_OBJECT => { 'prediction_interval': 0.95 }
);








