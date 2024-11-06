CREATE TABLE avg_age_per_category AS
SELECT 
    product_category,
    AVG(age) AS average_age
FROM 
    Data_Transaksi_Keuangan_E_commerce
GROUP BY 
    product_category
EMIT CHANGES;