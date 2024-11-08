import pyspark
import os
from dotenv import load_dotenv
from pathlib import Path
from pyspark.sql.functions import from_json, col, avg, sum, max, count, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DoubleType, BooleanType

# Memuat variabel lingkungan dari file .env untuk mendapatkan konfigurasi Spark dan Kafka
dotenv_path = Path("/opt/app/.env")
load_dotenv(dotenv_path=dotenv_path)

# Mendapatkan nama host dan port Spark Master serta host Kafka dari variabel lingkungan
spark_hostname = os.getenv("SPARK_MASTER_HOST_NAME")
spark_port = os.getenv("SPARK_MASTER_PORT")
kafka_host = os.getenv("KAFKA_HOST")
kafka_topic = "Data_Transaksi_Kartu_Kredit"  # Nama topik Kafka yang akan digunakan

# Menetapkan host Spark sebagai "local" untuk menjalankan Spark pada mode lokal
spark_host = "local"

# Menambahkan dependensi untuk koneksi ke Kafka dan PostgreSQL
os.environ["PYSPARK_SUBMIT_ARGS"] = (
    "--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2 org.postgresql:postgresql:42.2.18"
)

# Membuat SparkSession untuk menjalankan aplikasi streaming dengan konfigurasi yang diperlukan
spark = (
    pyspark.sql.SparkSession.builder.appName("Spark_Dibimbing_Streaming")
    .master(spark_host)
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0")
    .config("spark.sql.shuffle.partitions", 5)  # Mengatur jumlah partisi untuk operasi shuffle
    # .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", True)  # Opsional: Menghapus lokasi checkpoint sementara
    .getOrCreate()
)

# Mengatur level log untuk Spark menjadi "WARN" untuk mengurangi output log yang ditampilkan
spark.sparkContext.setLogLevel("WARN")

# Mendefinisikan skema untuk data transaksi kartu kredit sesuai dengan struktur data yang diharapkan
transaction_credit_card_schema = StructType([
    StructField("transaction_id", StringType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("user_id", StringType(), True),
    StructField("account_id", StringType(), True),
    StructField("transaction_type", StringType(), True),
    StructField("transaction_amount", DoubleType(), True),
    StructField("currency", StringType(), True),
    StructField("merchant_id", StringType(), True),
    StructField("name_merchant", StringType(), True),
    StructField("category", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("country", StringType(), True),
    StructField("zipcode", StringType(), True),
    StructField("phone", StringType(), True),
    StructField("email", StringType(), True),
    StructField("card_id", StringType(), True),
    StructField("card_type", StringType(), True),
    StructField("card_network", StringType(), True),
    StructField("expiration_date", StringType(), True),
    StructField("last_four_digits", IntegerType(), True),
    StructField("fraud_score", DoubleType(), True),
    StructField("is_suspicious", BooleanType(), True),
    StructField("transaction_status", StringType(), True),
    StructField("transaction_method", StringType(), True),
    StructField("device_type", StringType(), True),
    StructField("os", StringType(), True),
    StructField("app_version", StringType(), True)
])

# Membaca data stream dari Kafka dengan format value sebagai string dan offset terbaru
stream_df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", f"{kafka_host}:9092")
    .option("subscribe", kafka_topic)
    .option("startingOffsets", "latest")
    .load()
)

# Parsing data JSON dan mengubahnya ke dalam struktur DataFrame Spark berdasarkan skema yang telah ditentukan
parsed_df = stream_df.selectExpr("CAST(value AS STRING)").\
            select(from_json(col("value"), transaction_credit_card_schema).
            alias("data")).\
            select("data.*")

# Mengubah kolom 'timestamp' menjadi tipe waktu
transaction_df = parsed_df.withColumn("timestamp", to_timestamp("timestamp"))

# Menghitung agregasi berdasarkan timestamp
real_time_transaction_timestamp_df = (
    transaction_df
    .groupBy("timestamp")  # Mengelompokkan data berdasarkan timestamp
    .agg(
        avg("transaction_amount").alias("avg_amount"),  # Rata-rata jumlah transaksi
        count("transaction_id").alias("transaction_count"),  # Total transaksi
        sum("transaction_amount").alias("total_amount"),  # Total dari semua transaksi
        max("transaction_amount").alias("max_amount")  # Nilai transaksi maksimum
    )
)

# Menghitung agregasi berdasarkan tipe transaksi
real_time_transaction_transaction_type_df = (
    transaction_df
    .groupBy("transaction_type")  # Mengelompokkan data berdasarkan tipe transaksi
    .agg(
        avg("transaction_amount").alias("avg_amount"),  # Rata-rata jumlah transaksi
        count("transaction_id").alias("transaction_count"),  # Total transaksi
        sum("transaction_amount").alias("total_amount"),  # Total dari semua transaksi
        max("transaction_amount").alias("max_amount")  # Nilai transaksi maksimum
    )
)

# Menghitung agregasi berdasarkan status transaksi
real_time_transaction_transaction_status_df = (
    transaction_df
    .groupBy("transaction_status")  # Mengelompokkan data berdasarkan status transaksi
    .agg(
        avg("transaction_amount").alias("avg_amount"),  # Rata-rata jumlah transaksi
        count("transaction_id").alias("transaction_count"),  # Total transaksi
        sum("transaction_amount").alias("total_amount"),  # Total dari semua transaksi
        max("transaction_amount").alias("max_amount")  # Nilai transaksi maksimum
    )
)

# Menghitung agregasi berdasarkan metode transaksi
real_time_transaction_transaction_method_df = (
    transaction_df
    .groupBy("transaction_method")  # Mengelompokkan data berdasarkan metode transaksi
    .agg(
        avg("transaction_amount").alias("avg_amount"),  # Rata-rata jumlah transaksi
        count("transaction_id").alias("transaction_count"),  # Total transaksi
        sum("transaction_amount").alias("total_amount"),  # Total dari semua transaksi
        max("transaction_amount").alias("max_amount")  # Nilai transaksi maksimum
    )
)

# Menampilkan hasil agregasi berdasarkan timestamp ke console setiap 10 menit
query_timestamp = real_time_transaction_timestamp_df.writeStream.\
                  outputMode("complete").\
                  format("console").\
                  trigger(processingTime="10 minutes").start()

# Menampilkan hasil agregasi berdasarkan tipe transaksi ke console setiap 10 menit
query_transaction_type = real_time_transaction_transaction_type_df.writeStream.\
                         outputMode("complete").\
                         format("console").\
                         trigger(processingTime="10 minutes").start()

# Menampilkan hasil agregasi berdasarkan status transaksi ke console setiap 10 menit
query_transaction_status = real_time_transaction_transaction_status_df.writeStream.\
                           outputMode("complete").\
                           format("console").\
                           trigger(processingTime="10 minutes").start()

# Menampilkan hasil agregasi berdasarkan metode transaksi ke console setiap 10 menit
query_transaction_method = real_time_transaction_transaction_method_df.writeStream.\
                           outputMode("complete").\
                           format("console").\
                           trigger(processingTime="10 minutes").start()

if __name__=="__main__":

    # Menjalankan streaming secara terus-menerus sampai program dihentikan
    query_timestamp.awaitTermination()
    query_transaction_type.awaitTermination()
    query_transaction_status.awaitTermination()
    query_transaction_method.awaitTermination()