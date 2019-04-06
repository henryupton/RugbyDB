DROP TABLE if exists reference.dim_date;

CREATE TABLE reference.dim_date
(
  date_dim_id              INT NOT NULL,
  date_actual              DATE NOT NULL,
  timestamp_actual         TIMESTAMP NOT NULL,
  epoch                    BIGINT NOT NULL,
  day_suffix               VARCHAR(4) NOT NULL,
  day_name                 VARCHAR(9) NOT NULL,
  day_of_week              INT NOT NULL,
  day_of_month             INT NOT NULL,
  day_of_year              INT NOT NULL,
  week_of_month            INT NOT NULL,
  week_of_year             INT NOT NULL,
  week_of_year_iso         CHAR(10) NOT NULL,
  month_actual             INT NOT NULL,
  month_name               VARCHAR(9) NOT NULL,
  month_name_abbreviated   CHAR(3) NOT NULL,
  year_actual              INT NOT NULL,
  first_day_of_week        DATE NOT NULL,
  last_day_of_week         DATE NOT NULL,
  first_day_of_month       DATE NOT NULL,
  last_day_of_month        DATE NOT NULL,
  first_day_of_year        DATE NOT NULL,
  last_day_of_year         DATE NOT NULL,
  mmyyyy                   CHAR(6) NOT NULL,
  mmddyyyy                 CHAR(10) NOT NULL,
  weekend_indr             BOOLEAN NOT NULL
);

ALTER TABLE reference.dim_date ADD CONSTRAINT d_date_date_dim_id_pk PRIMARY KEY (date_dim_id);

CREATE INDEX d_date_date_actual_idx
  ON reference.dim_date(date_actual);

COMMIT;

INSERT INTO reference.dim_date
SELECT
    TO_CHAR(datum,'yyyymmdd')::INT AS date_dim_id,
    datum AS date_actual,
    datum::TIMESTAMP AS datum_ts,
    EXTRACT(epoch FROM datum) AS epoch,
    TO_CHAR(datum,'fmDDth') AS day_suffix,
    TO_CHAR(datum,'Day') AS day_name,
    EXTRACT(isodow FROM datum) AS day_of_week,
    EXTRACT(DAY FROM datum) AS day_of_month,
    EXTRACT(doy FROM datum) AS day_of_year,
    TO_CHAR(datum,'W')::INT AS week_of_month,
    EXTRACT(week FROM datum) AS week_of_year,
    TO_CHAR(datum,'YYYY"-W"IW-') || EXTRACT(isodow FROM datum) AS week_of_year_iso,
    EXTRACT(MONTH FROM datum) AS month_actual,
    TO_CHAR(datum,'Month') AS month_name,
    TO_CHAR(datum,'Mon') AS month_name_abbreviated,
    EXTRACT(isoyear FROM datum) AS year_actual,
    datum +(1 -EXTRACT(isodow FROM datum))::INT AS first_day_of_week,
    datum +(7 -EXTRACT(isodow FROM datum))::INT AS last_day_of_week,
    datum +(1 -EXTRACT(DAY FROM datum))::INT AS first_day_of_month,
    (DATE_TRUNC('MONTH',datum) +INTERVAL '1 MONTH - 1 day')::DATE AS last_day_of_month,
    TO_DATE(EXTRACT(isoyear FROM datum) || '-01-01','YYYY-MM-DD') AS first_day_of_year,
    TO_DATE(EXTRACT(isoyear FROM datum) || '-12-31','YYYY-MM-DD') AS last_day_of_year,
    TO_CHAR(datum,'mmyyyy') AS mmyyyy,
    TO_CHAR(datum,'mmddyyyy') AS mmddyyyy,
    CASE
     WHEN EXTRACT(isodow FROM datum) IN (6,7) THEN TRUE
     ELSE FALSE
    END AS weekend_indr
FROM
(SELECT
    CAST('2010-01-01' AS DATE) + SEQUENCE.DAY AS datum
FROM GENERATE_SERIES(0,4000) AS SEQUENCE(DAY)
GROUP BY SEQUENCE.DAY) DQ
ORDER BY 1;

COMMIT;
