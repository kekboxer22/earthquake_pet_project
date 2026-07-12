-- Схемы, которые ожидают DAG'и
CREATE SCHEMA IF NOT EXISTS ods;
CREATE SCHEMA IF NOT EXISTS stg;
CREATE SCHEMA IF NOT EXISTS dm;

-- Витрина: количество землетрясений по дням
CREATE TABLE IF NOT EXISTS dm.fct_count_day_earthquake (
    date DATE PRIMARY KEY,
    cnt  BIGINT
);

-- Витрина: средняя магнитуда по дням
CREATE TABLE IF NOT EXISTS dm.fct_avg_day_earthquake (
    date    DATE PRIMARY KEY,
    avg_mag DOUBLE PRECISION
);
