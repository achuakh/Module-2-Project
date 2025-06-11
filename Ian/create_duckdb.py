import duckdb

con = duckdb.connect("br_temp.db")

con.sql(
    """
DROP SCHEMA IF EXISTS OLIST CASCADE;

CREATE SCHEMA OLIST ;

SET SEARCH_PATH TO OLIST;

CREATE TABLE OLIST.DIM_CUSTOMERS AS
SELECT *, 
strftime('%Y-%m-%d %I:%M:%S %p', CAST(NOW() AS TIMESTAMP)) AS load_date
FROM read_csv_auto('/home/chuhao/dsai_sctp/br_e_commerce/data/olist_customers_dataset.csv');

CREATE TABLE OLIST.DIM_PAYMENTS AS
SELECT *, 
strftime('%Y-%m-%d %I:%M:%S %p', CAST(NOW() AS TIMESTAMP)) AS load_date
FROM read_csv_auto('/home/chuhao/dsai_sctp/br_e_commerce/data/olist_order_payments_dataset.csv');

CREATE TABLE OLIST.DIM_REVIEWS AS
SELECT *, 
strftime('%Y-%m-%d %I:%M:%S %p', CAST(NOW() AS TIMESTAMP)) AS load_date
FROM read_csv_auto('/home/chuhao/dsai_sctp/br_e_commerce/data/olist_order_reviews_dataset.csv');

CREATE TABLE OLIST.TEMP_GEOLOCATION AS
SELECT * 
FROM read_csv_auto('/home/chuhao/dsai_sctp/br_e_commerce/data/olist_geolocation_dataset.csv');

CREATE TABLE OLIST.DIM_GEOLOCATION AS
SELECT DISTINCT * FROM TEMP_GEOLOCATION ; 

DROP TABLE IF EXISTS OLIST.TEMP_GEOLOCATION; 

CREATE TABLE OLIST.TEMP_ORDER_ITEMS AS
SELECT *, 
strftime('%Y-%m-%d %I:%M:%S %p', CAST(NOW() AS TIMESTAMP)) AS load_date
FROM read_csv_auto('/home/chuhao/dsai_sctp/br_e_commerce/data/olist_order_items_dataset.csv');

CREATE TABLE OLIST.TEMP_PRODUCTS AS
SELECT *, 
strftime('%Y-%m-%d %I:%M:%S %p', CAST(NOW() AS TIMESTAMP)) AS load_date
FROM read_csv_auto('/home/chuhao/dsai_sctp/br_e_commerce/data/olist_products_dataset.csv');

CREATE TABLE OLIST.TEMP_SELLERS AS
SELECT *, 
strftime('%Y-%m-%d %I:%M:%S %p', CAST(NOW() AS TIMESTAMP)) AS load_date
FROM read_csv_auto('/home/chuhao/dsai_sctp/br_e_commerce/data/olist_sellers_dataset.csv');

CREATE TABLE OLIST.TEMP_PRODUCT_CATEGORY_NAME_TRANSLATION AS
SELECT *, 
strftime('%Y-%m-%d %I:%M:%S %p', CAST(NOW() AS TIMESTAMP)) AS load_date
FROM read_csv_auto('/home/chuhao/dsai_sctp/br_e_commerce/data/product_category_name_translation.csv');



CREATE TABLE DIM_ITEMS AS 
SELECT  
    I.order_id,
    I.order_item_id,
    I.product_id,
    I.seller_id,
    I.shipping_limit_date,
    I.price,
    I.freight_value,
    P.product_category_name,
    C.product_category_name_english,
    P.product_name_lenght as product_name_length,
    P.product_description_lenght as product_description_length,
    P.product_photos_qty,
    P.product_weight_g,
    P.product_length_cm,
    P.product_height_cm,
    P.product_width_cm,
    S.seller_zip_code_prefix,
    S.seller_city,
    S.seller_state
FROM OLIST.TEMP_ORDER_ITEMS I
LEFT JOIN OLIST.TEMP_PRODUCTS P ON I.product_id = P.product_id
LEFT JOIN OLIST.TEMP_PRODUCT_CATEGORY_NAME_TRANSLATION C ON P.product_category_name = C.product_category_name
LEFT JOIN OLIST.TEMP_SELLERS S ON I.seller_id = S.seller_id;


ALTER TABLE DIM_CUSTOMERS
RENAME COLUMN customer_id TO customer_sid ;

ALTER TABLE DIM_PAYMENTS
RENAME COLUMN order_id TO payment_sid ;

ALTER TABLE DIM_REVIEWS
RENAME COLUMN order_id TO review_sid ;

ALTER TABLE DIM_ITEMS
RENAME COLUMN order_id TO item_sid ;

CREATE TABLE OLIST.TEMP_ORDERS AS
SELECT *, 
strftime('%Y-%m-%d %I:%M:%S %p', CAST(NOW() AS TIMESTAMP)) AS load_date
FROM read_csv_auto('/home/chuhao/dsai_sctp/br_e_commerce/data/olist_orders_dataset.csv');


CREATE TABLE OLIST.FCT_ORDERS AS 
SELECT 
    O.order_id AS payment_sid, 
    O.order_id AS review_sid, 
    O.order_id AS item_sid, 
    O.customer_id AS customer_sid, 
    O.order_status, 
    O.order_purchase_timestamp, 
    O.order_approved_at, 
    O.order_delivered_carrier_date, 
    O.order_delivered_customer_date, 
    O.order_estimated_delivery_date,
    COALESCE(P.TOTAL_PAYMENT, 0) AS TOTAL_PAYMENT
FROM OLIST.TEMP_ORDERS O
LEFT JOIN (
    SELECT PAYMENT_SID, SUM(PAYMENT_VALUE) AS TOTAL_PAYMENT
    FROM DIM_PAYMENTS
    GROUP BY PAYMENT_SID
) P ON O.order_id = P.PAYMENT_SID;


DROP TABLE IF EXISTS OLIST.TEMP_GEOLOCATION; 
DROP TABLE IF EXISTS OLIST.TEMP_ORDER_ITEMS;
DROP TABLE IF EXISTS OLIST.TEMP_PRODUCTs;
DROP TABLE IF EXISTS OLIST.TEMP_PRODUCT_CATEGORY_NAME_TRANSLATION;
DROP TABLE IF EXISTS OLIST.TEMP_SELLERS;
DROP TABLE IF EXISTS OLIST.TEMP_ORDERS;


CREATE TABLE DIM_DATE AS
WITH RECURSIVE date_series AS (
    SELECT DATE '2000-01-01' AS date_value
    UNION ALL
    SELECT date_value + INTERVAL 1 DAY
    FROM date_series
    WHERE date_value < DATE '2050-12-31'
)
SELECT
    date_value AS date_key,
    date_value AS full_date,
    EXTRACT(YEAR FROM date_value) AS year,
    EXTRACT(MONTH FROM date_value) AS month,
    EXTRACT(DAY FROM date_value) AS day,
    EXTRACT(QUARTER FROM date_value) AS quarter,
    EXTRACT(WEEK FROM date_value) AS week,
    EXTRACT(DAYOFWEEK FROM date_value) AS day_of_week,
    strftime('%A', date_value) AS day_name,
    strftime('%B', date_value) AS month_name,
    strftime('%Y-%m', date_value) AS year_month,
    CONCAT(strftime('%Y', date_value), '-Q', EXTRACT(QUARTER FROM date_value)) AS year_quarter
FROM date_series; 
    """
)