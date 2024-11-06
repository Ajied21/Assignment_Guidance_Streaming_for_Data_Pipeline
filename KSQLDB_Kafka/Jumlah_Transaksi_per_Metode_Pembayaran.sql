CREATE TABLE payment_method_counts AS
SELECT 
    payment_method,
    COUNT(*) AS transaction_count
FROM 
    Data_Transaksi_Keuangan_E_commerce
GROUP BY 
    payment_method
EMIT CHANGES;
