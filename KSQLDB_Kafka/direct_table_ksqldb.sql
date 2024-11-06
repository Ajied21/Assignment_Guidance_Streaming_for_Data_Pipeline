SELECT * FROM Data_Transaksi_Keuangan_E_commerce EMIT CHANGES;

-- Untuk melihat pendapatan per merchant
SELECT * FROM merchant_revenue EMIT CHANGES;

-- Untuk melihat rata-rata usia per kategori produk
SELECT * FROM avg_age_per_category EMIT CHANGES;

-- Untuk melihat jumlah transaksi per metode pembayaran
SELECT * FROM payment_method_counts EMIT CHANGES;
