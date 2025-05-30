DROP SCHEMA NTUdb.OList CASCADE ;

CREATE SCHEMA NTUdb.OList ;

SET search_path TO OList;

CREATE TABLE OLIST.DIM_CUSTOMERS AS
SELECT *, 
strftime('%Y-%m-%d %I:%M:%S %p', CAST(NOW() AS TIMESTAMP)) AS load_date
FROM read_csv_auto('/home/swlai/NTU_DSAI/Project/Olist/olist_customers_dataset.csv');

CREATE TABLE OLIST.DIM_ORDER_ITEMS AS
SELECT *, 
strftime('%Y-%m-%d %I:%M:%S %p', CAST(NOW() AS TIMESTAMP)) AS load_date
FROM read_csv_auto('/home/swlai/NTU_DSAI/Project/Olist/olist_order_items_dataset.csv');

CREATE TABLE OLIST.DIM_ORDER_PAYMENTS AS
SELECT *, 
strftime('%Y-%m-%d %I:%M:%S %p', CAST(NOW() AS TIMESTAMP)) AS load_date
FROM read_csv_auto('/home/swlai/NTU_DSAI/Project/Olist/olist_order_payments_dataset.csv');

CREATE TABLE OLIST.DIM_ORDER_REVIEWS AS
SELECT *, 
strftime('%Y-%m-%d %I:%M:%S %p', CAST(NOW() AS TIMESTAMP)) AS load_date
FROM read_csv_auto('/home/swlai/NTU_DSAI/Project/Olist/olist_order_reviews_dataset.csv');

CREATE TABLE OLIST.FCT_ORDERS AS
SELECT *, 
strftime('%Y-%m-%d %I:%M:%S %p', CAST(NOW() AS TIMESTAMP)) AS load_date
FROM read_csv_auto('/home/swlai/NTU_DSAI/Project/Olist/olist_orders_dataset.csv');

CREATE TABLE OLIST.DIM_PRODUCTS AS
SELECT *, 
strftime('%Y-%m-%d %I:%M:%S %p', CAST(NOW() AS TIMESTAMP)) AS load_date
FROM read_csv_auto('/home/swlai/NTU_DSAI/Project/Olist/olist_products_dataset.csv');

CREATE TABLE OLIST.DIM_SELLERS AS
SELECT *, 
strftime('%Y-%m-%d %I:%M:%S %p', CAST(NOW() AS TIMESTAMP)) AS load_date
FROM read_csv_auto('/home/swlai/NTU_DSAI/Project/Olist/olist_sellers_dataset.csv');

CREATE TABLE OLIST.DIM_PRODUCT_CATEGORY_NAME_TRANSLATION AS
SELECT *, 
strftime('%Y-%m-%d %I:%M:%S %p', CAST(NOW() AS TIMESTAMP)) AS load_date
FROM read_csv_auto('/home/swlai/NTU_DSAI/Project/Olist/product_category_name_translation.csv');

CREATE TABLE OLIST.TEMP_GEOLOCATION AS
SELECT * 
FROM read_csv_auto('/home/swlai/NTU_DSAI/Project/Olist/olist_geolocation_dataset.csv');

CREATE TABLE OLIST.DIM_GEOLOCATION AS
SELECT DISTINCT * FROM TEMP_GEOLOCATION ; 

DROP TABLE IF EXISTS OLIST.TEMP_GEOLOCATION; 



