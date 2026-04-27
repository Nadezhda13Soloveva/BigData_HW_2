TRUNCATE TABLE fact_sales RESTART IDENTITY;

INSERT INTO dim_customer
SELECT DISTINCT sale_customer_id, customer_first_name, customer_last_name, customer_age,
       customer_email, customer_country, customer_postal_code, customer_pet_type,
       customer_pet_name, customer_pet_breed
FROM mock_data WHERE sale_customer_id IS NOT NULL
ON CONFLICT (customer_id) DO NOTHING;

INSERT INTO dim_seller
SELECT DISTINCT sale_seller_id, seller_first_name, seller_last_name, seller_email,
       seller_country, seller_postal_code
FROM mock_data WHERE sale_seller_id IS NOT NULL
ON CONFLICT (seller_id) DO NOTHING;

INSERT INTO dim_product
SELECT DISTINCT sale_product_id, product_name, product_category, product_price, product_quantity,
       product_weight, product_color, product_size, product_brand, product_material,
       product_description, product_rating, product_reviews,
       NULLIF(product_release_date, '')::DATE,
       NULLIF(product_expiry_date, '')::DATE
FROM mock_data WHERE sale_product_id IS NOT NULL
ON CONFLICT (product_id) DO NOTHING;

INSERT INTO dim_store (name, location, city, state, country, phone, email)
SELECT DISTINCT store_name, store_location, store_city, store_state, store_country, store_phone, store_email
FROM mock_data WHERE store_name IS NOT NULL
ON CONFLICT (name) DO NOTHING;

INSERT INTO dim_supplier (name, contact, email, phone, address, city, country)
SELECT DISTINCT supplier_name, supplier_contact, supplier_email, supplier_phone,
       supplier_address, supplier_city, supplier_country
FROM mock_data WHERE supplier_name IS NOT NULL
ON CONFLICT (name) DO NOTHING;

INSERT INTO fact_sales 
    (source_sale_id, sale_date, customer_id, seller_id, product_id, store_id, supplier_id, quantity, total_price)
SELECT 
    m.id AS source_sale_id,
    TO_DATE(m.sale_date, 'MM/DD/YYYY'),
    m.sale_customer_id,
    m.sale_seller_id,
    m.sale_product_id,
    st.store_id,
    sup.supplier_id,
    m.sale_quantity,
    m.sale_total_price
FROM mock_data m
LEFT JOIN dim_store st ON st.name = m.store_name
LEFT JOIN dim_supplier sup ON sup.name = m.supplier_name
WHERE m.id IS NOT NULL;
DROP TABLE IF EXISTS mock_data;

SELECT 'dim_customer'  AS table_name, COUNT(*) FROM dim_customer UNION ALL
SELECT 'dim_seller'    AS table_name, COUNT(*) FROM dim_seller UNION ALL
SELECT 'dim_product'   AS table_name, COUNT(*) FROM dim_product UNION ALL
SELECT 'dim_store'     AS table_name, COUNT(*) FROM dim_store UNION ALL
SELECT 'dim_supplier'  AS table_name, COUNT(*) FROM dim_supplier UNION ALL
SELECT 'fact_sales'    AS table_name, COUNT(*) FROM fact_sales
ORDER BY table_name;