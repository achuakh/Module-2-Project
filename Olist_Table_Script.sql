DROP SCHEMA IF EXISTS OLIST CASCADE;

CREATE SCHEMA OLIST ;

SET SEARCH_PATH TO OLIST;

CREATE TABLE OLIST.DIM_CUSTOMERS AS
SELECT *, 
strftime('%Y-%m-%d %I:%M:%S %p', CAST(NOW() AS TIMESTAMP)) AS load_date
FROM read_csv_auto('/home/swlai/NTU_DSAI/Project/Olist/olist_customers_dataset.csv');

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

CREATE TABLE OLIST.TEMP_GEOLOCATION AS
SELECT * 
FROM read_csv_auto('/home/swlai/NTU_DSAI/Project/Olist/olist_geolocation_dataset.csv');

CREATE TABLE OLIST.DIM_GEOLOCATION AS
SELECT DISTINCT * FROM TEMP_GEOLOCATION ; 

DROP TABLE IF EXISTS OLIST.TEMP_GEOLOCATION; 

CREATE TABLE OLIST.TEMP_ORDER_ITEMS AS
SELECT *, 
strftime('%Y-%m-%d %I:%M:%S %p', CAST(NOW() AS TIMESTAMP)) AS load_date
FROM read_csv_auto('/home/swlai/NTU_DSAI/Project/Olist/olist_order_items_dataset.csv');

CREATE TABLE OLIST.TEMP_PRODUCTS AS
SELECT *, 
strftime('%Y-%m-%d %I:%M:%S %p', CAST(NOW() AS TIMESTAMP)) AS load_date
FROM read_csv_auto('/home/swlai/NTU_DSAI/Project/Olist/olist_products_dataset.csv');

CREATE TABLE OLIST.TEMP_SELLERS AS
SELECT *, 
strftime('%Y-%m-%d %I:%M:%S %p', CAST(NOW() AS TIMESTAMP)) AS load_date
FROM read_csv_auto('/home/swlai/NTU_DSAI/Project/Olist/olist_sellers_dataset.csv');

CREATE TABLE OLIST.TEMP_PRODUCT_CATEGORY_NAME_TRANSLATION AS
SELECT *, 
strftime('%Y-%m-%d %I:%M:%S %p', CAST(NOW() AS TIMESTAMP)) AS load_date
FROM read_csv_auto('/home/swlai/NTU_DSAI/Project/Olist/product_category_name_translation.csv');

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
    P.product_name_length,
    P.product_description_length,
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

DROP TABLE IF EXISTS OLIST.TEMP_GEOLOCATION; 
DROP TABLE IF EXISTS OLIST.TEMP_ORDER_ITEMS;
DROP TABLE IF EXISTS OLIST.TEMP_PRODUCT;
DROP TABLE IF EXISTS OLIST.TEMP_PRODUCT_CATEGORY_NAME_TRANSLATION;
DROP TABLE IF EXISTS OLIST.TEMP_SELLERS;


