DROP TABLE IF EXISTS fact_sales CASCADE;
DROP TABLE IF EXISTS dim_customer CASCADE;
DROP TABLE IF EXISTS dim_seller CASCADE;
DROP TABLE IF EXISTS dim_product CASCADE;
DROP TABLE IF EXISTS dim_store CASCADE;
DROP TABLE IF EXISTS dim_supplier CASCADE;

CREATE TABLE dim_customer (
    customer_id INTEGER PRIMARY KEY,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    age INTEGER,
    email VARCHAR(100),
    country VARCHAR(50),
    postal_code VARCHAR(20),
    pet_type VARCHAR(20),
    pet_name VARCHAR(50),
    pet_breed VARCHAR(50)
);

CREATE TABLE dim_seller (
    seller_id INTEGER PRIMARY KEY,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    email VARCHAR(100),
    country VARCHAR(50),
    postal_code VARCHAR(20)
);

CREATE TABLE dim_product (
    product_id INTEGER PRIMARY KEY,
    name VARCHAR(100),
    category VARCHAR(50),
    price NUMERIC(10,2),
    stock_quantity INTEGER,
    weight NUMERIC(5,2),
    color VARCHAR(30),
    size VARCHAR(20),
    brand VARCHAR(50),
    material VARCHAR(50),
    description TEXT,
    rating NUMERIC(3,1),
    reviews INTEGER,
    release_date DATE,
    expiry_date DATE
);

CREATE TABLE dim_store (
    store_id SERIAL PRIMARY KEY,
    name VARCHAR(100) UNIQUE,
    location TEXT,
    city VARCHAR(50),
    state VARCHAR(50),
    country VARCHAR(50),
    phone VARCHAR(30),
    email VARCHAR(100)
);

CREATE TABLE dim_supplier (
    supplier_id SERIAL PRIMARY KEY,
    name VARCHAR(100) UNIQUE,
    contact VARCHAR(100),
    email VARCHAR(100),
    phone VARCHAR(30),
    address TEXT,
    city VARCHAR(50),
    country VARCHAR(50)
);

CREATE TABLE fact_sales (
    sale_id BIGSERIAL PRIMARY KEY,
    source_sale_id BIGINT,  -- оригинальный id из CSV (может дублироваться)
    sale_date DATE NOT NULL,
    customer_id INTEGER REFERENCES dim_customer(customer_id),
    seller_id INTEGER REFERENCES dim_seller(seller_id),
    product_id INTEGER REFERENCES dim_product(product_id),
    store_id INTEGER REFERENCES dim_store(store_id),
    supplier_id INTEGER REFERENCES dim_supplier(supplier_id),
    quantity INTEGER NOT NULL,
    total_price NUMERIC(12,2) NOT NULL
);