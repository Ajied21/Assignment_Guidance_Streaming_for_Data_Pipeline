CREATE STREAM Data_Transaksi_Keuangan_E_commerce (
    transaction_id STRING,
    timestamp STRING,
    user_id STRING,
    merchant_id STRING,
    amount DOUBLE,
    age INT,
    gender STRING,
    phone_number STRING,
    email STRING,
    currency STRING,
    location STRING,
    country STRING,
    payment_method STRING,
    product_id STRING,
    product_category STRING,
    quantity INT,
    discount DOUBLE,
    tax DOUBLE,
    shipping_cost DOUBLE,
    total_cost DOUBLE
) WITH (
    KAFKA_TOPIC='Data_Transaksi_Keuangan_E-commerce',
    VALUE_FORMAT='JSON'
);
