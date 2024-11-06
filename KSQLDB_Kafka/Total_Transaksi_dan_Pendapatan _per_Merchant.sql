CREATE TABLE merchant_revenue AS
SELECT 
    merchant_id,
    COUNT(*) AS transaction_count,
    SUM(total_cost) AS total_revenue
FROM 
    Data_Transaksi_Keuangan_E_commerce
GROUP BY 
    merchant_id
EMIT CHANGES;
