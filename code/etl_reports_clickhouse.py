from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import requests

spark = SparkSession.builder \
    .appName("BigDataLab2_Reports") \
    .config("spark.jars.packages", 
            "org.postgresql:postgresql:42.7.1,"
            "org.mongodb.spark:mongo-spark-connector_2.12:10.3.0") \
    .getOrCreate()

# чтение schema из PG
jdbc_url_pg = "jdbc:postgresql://postgres_lab:5432/dbspark_lab"

properties_pg = {
    "user": "user",
    "password": "userpass",
    "driver": "org.postgresql.Driver"
}

dim_customer = spark.read.jdbc(jdbc_url_pg, "dim_customer", properties=properties_pg)
dim_product = spark.read.jdbc(jdbc_url_pg, "dim_product", properties=properties_pg)
dim_seller = spark.read.jdbc(jdbc_url_pg, "dim_seller", properties=properties_pg)
dim_store = spark.read.jdbc(jdbc_url_pg, "dim_store", properties=properties_pg)
dim_supplier = spark.read.jdbc(jdbc_url_pg, "dim_supplier", properties=properties_pg)
fact_sales = spark.read.jdbc(jdbc_url_pg, "fact_sales", properties=properties_pg)

# ClickHouse настройки
CH_URL = "http://clickhouse_lab:8123"
CH_PARAMS = {
    "database": "lab_reports",
    "user": "lab_user",
    "password": "lab_pass"
}

# MongoDB настройки
MONGO_URI = "mongodb://mongodb:27017"
MONGO_DB = "lab_reports"


def ch_execute(query):
    response = requests.post(CH_URL, params=CH_PARAMS, data=query)
    if response.status_code != 200:
        print(f"ClickHouse error: {response.text}")
    return response


def save_to_clickhouse(df, table_name):
    pdf = df.toPandas()
    columns = pdf.columns.tolist()
    ch_execute(f"DROP TABLE IF EXISTS {table_name}")

    cols_def = ', '.join([f"`{c}` String" for c in columns])
    ch_execute(f"CREATE TABLE {table_name} ({cols_def}) ENGINE = MergeTree() ORDER BY tuple()")

    # вставляем данные батчами по 100 строк
    batch_size = 100
    for i in range(0, len(pdf), batch_size):
        batch = pdf.iloc[i:i+batch_size]
        values_list = []
        for _, row in batch.iterrows():
            values = ', '.join([f"'{str(v).replace(chr(39), chr(39)+chr(39))}'" for v in row.values])
            values_list.append(f"({values})")

        query = f"INSERT INTO {table_name} VALUES {', '.join(values_list)}"
        ch_execute(query)

    print(f"  ✓ {table_name} сохранено в ClickHouse ({len(pdf)} записей)")


def save_to_mongodb(df, collection_name):
    df.write \
        .format("mongodb") \
        .option("connection.uri", MONGO_URI) \
        .option("database", MONGO_DB) \
        .option("collection", collection_name) \
        .mode("overwrite") \
        .save()
    print(f"  ✓ {collection_name} сохранено в MongoDB")


print("\n[1/6] Витрина по продуктам")
v_products = fact_sales.join(dim_product, "product_id") \
    .groupBy("name", "category") \
    .agg(
        sum("total_price").alias("total_revenue"),
        count("*").alias("num_sales"),
        avg("rating").alias("avg_rating"),
        sum("reviews").alias("total_reviews")
    ).orderBy(desc("total_revenue"))

save_to_clickhouse(v_products, "v_products")
save_to_mongodb(v_products, "v_products")


print("\n[2/6] Витрина по клиентам")
v_customers = fact_sales.join(dim_customer, "customer_id") \
    .groupBy("first_name", "last_name", "country") \
    .agg(
        sum("total_price").alias("total_spent"),
        count("*").alias("num_purchases"),
        avg("total_price").alias("avg_check")
    ).orderBy(desc("total_spent"))

save_to_clickhouse(v_customers, "v_customers")
save_to_mongodb(v_customers, "v_customers")


print("\n[3/6] Витрина по времени")
v_time = fact_sales.withColumn("sale_year", year("sale_date")) \
    .withColumn("sale_month", month("sale_date")) \
    .groupBy("sale_year", "sale_month") \
    .agg(
        sum("total_price").alias("monthly_revenue"),
        count("*").alias("num_sales"),
        avg("total_price").alias("avg_order")
    ).orderBy("sale_year", "sale_month")

save_to_clickhouse(v_time, "v_time")
save_to_mongodb(v_time, "v_time")


print("\n[4/6] Витрина по магазинам")
v_stores = fact_sales.join(dim_store, "store_id") \
    .groupBy("name", "city", "country") \
    .agg(
        sum("total_price").alias("total_revenue"),
        count("*").alias("num_sales"),
        avg("total_price").alias("avg_check")
    ).orderBy(desc("total_revenue"))

save_to_clickhouse(v_stores, "v_stores")
save_to_mongodb(v_stores, "v_stores")


print("\n[5/6] Витрина по поставщикам")

# алиасы для избежания конфликта имён
supplier_alias = dim_supplier.alias("supp")
product_alias = dim_product.alias("prod")

v_suppliers = fact_sales \
    .join(product_alias, "product_id") \
    .join(supplier_alias, "supplier_id") \
    .groupBy(col("supp.name").alias("supplier_name"), col("supp.country").alias("supplier_country")) \
    .agg(
        sum("total_price").alias("total_revenue"),
        avg(col("prod.price")).alias("avg_price")
    ).orderBy(desc("total_revenue"))

save_to_clickhouse(v_suppliers, "v_suppliers")
save_to_mongodb(v_suppliers, "v_suppliers")

print("\n[6/6] Витрина качества")
v_quality = fact_sales.join(dim_product, "product_id") \
    .groupBy("name", "category") \
    .agg(
        avg("rating").alias("avg_rating"),
        sum("reviews").alias("total_reviews"),
        sum("total_price").alias("total_revenue"),
        count("*").alias("num_sales")
    ).orderBy(desc("avg_rating"))

save_to_clickhouse(v_quality, "v_quality")
save_to_mongodb(v_quality, "v_quality")


print("Все 6 витрин загружены в ClickHouse")
print("Все 6 витрин загружены в MongoDB!")

spark.stop()
