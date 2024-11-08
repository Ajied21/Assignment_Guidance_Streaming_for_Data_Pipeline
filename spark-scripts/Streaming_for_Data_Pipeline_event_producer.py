import json
import uuid
import os
from dotenv import load_dotenv
from pathlib import Path
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from faker import Faker
from time import sleep
from datetime import datetime, timedelta

# Memuat file .env yang berisi konfigurasi seperti alamat Kafka
dotenv_path = Path("/opt/app/.env")
load_dotenv(dotenv_path=dotenv_path)

# Mengambil host Kafka dari variabel lingkungan
kafka_host = os.getenv("KAFKA_HOST")
kafka_topic = "Data_Transaksi_Kartu_Kredit"  # Nama topik Kafka yang akan digunakan

# Membuat instance KafkaProducer untuk mengirim data ke Kafka
producer = KafkaProducer(bootstrap_servers=f"{kafka_host}:9092")
faker = Faker()  # Menggunakan Faker untuk menghasilkan data palsu


class TransactionDataGenerator(object):
    # Metode statis untuk menghasilkan data transaksi palsu
    @staticmethod
    def get_data():
        now = datetime.now() # Mendapatkan timestamp saat ini
        return {
            "transaction_id": f"{uuid.uuid4()}",  # ID transaksi unik
            "timestamp": faker.unix_time(start_datetime=now - timedelta(minutes=60), end_datetime=now),  # Waktu transaksi
            "user_id": f"{faker.random_int(min=100, max=999)}",  # ID pengguna
            "account_id": f"account_{faker.random_int(min=10000, max=99999)}",  # ID akun
            "transaction_type": faker.random_element(elements=("purchase", "refund", "transfer")),  # Jenis transaksi
            "transaction_amount": round(faker.random_number(digits=3) + faker.random_int(min=1000, max=100000), 2),  # Jumlah transaksi
            "currency": faker.random_element(elements=("USD", "EUR", "GBP", "INR", "IDN")),  # Mata uang
            "merchant_id": f"{uuid.uuid4()}",  # ID merchant unik
            "name_merchant": faker.company(),  # Nama merchant
            "category": faker.random_element(elements=("Retail", "Services", "Grocery", "Electronics")),  # Kategori merchant
            "city": faker.city(),  # Kota merchant
            "state": faker.state_abbr(),  # Singkatan provinsi
            "country": faker.country(),  # Negara merchant
            "zipcode": faker.zipcode(),  # Kode pos merchant
            "phone": faker.phone_number(),  # Nomor telepon merchant
            "email": faker.email(),  # Email merchant
            "card_id": f"card_{faker.random_int(min=10000, max=99999)}",  # ID kartu pembayaran
            "card_type": faker.random_element(elements=("credit", "debit")),  # Jenis kartu (kredit/debit)
            "card_network": faker.random_element(elements=("Visa", "MasterCard", "American Express", "Credit of BCA", "Credit of BRI", "Credit of BNI", "Credit of Mandiri")),  # Jaringan kartu
            "expiration_date": f"{faker.random_int(min=1, max=12):02d}/{faker.random_int(min=25, max=30)}",  # Tanggal kadaluarsa kartu
            "last_four_digits": faker.random_int(min=1000, max=9999),  # 4 digit terakhir dari nomor kartu
            "fraud_score": round(faker.random_number(digits=1) / 2, 1),  # Skor kecurangan (fraud score)
            "is_suspicious": faker.boolean(),  # Apakah transaksi mencurigakan
            "transaction_status": faker.random_element(elements=("completed", "pending", "failed")),  # Status transaksi
            "transaction_method": faker.random_element(elements=("online", "in-store", "ATM")),  # Metode transaksi
            "device_type": faker.random_element(elements=("smartphone", "laptop", "tablet")),  # Tipe perangkat yang digunakan
            "os": faker.random_element(elements=("Android", "iOS", "Windows", "Linux")),  # Sistem operasi perangkat
            "app_version": f"v{faker.random_int(min=1, max=5)}.{faker.random_int(min=0, max=9)}.{faker.random_int(min=0, max=9)}"  # Versi aplikasi
        }

# Membuat instance KafkaAdminClient untuk mengelola topik Kafka
admin_client = KafkaAdminClient(bootstrap_servers=f'{kafka_host}:9092')

# Mendefinisikan jumlah partisi dan faktor replikasi untuk topik Kafka yang akan dibuat
partitions = 5  # Jumlah partisi
replication_factor = 1  # Faktor replikasi, sesuaikan dengan kebutuhan

# Membuat objek NewTopic untuk mendefinisikan topik baru
new_topic = NewTopic(name=kafka_topic, num_partitions=partitions, replication_factor=replication_factor)


if __name__=="__main__":

    # Mencoba membuat topik baru di Kafka
    try:
        admin_client.create_topics([new_topic])  # Membuat topik
        print(f"Topik '{kafka_topic}' berhasil dibuat dengan {partitions} partisi.\n")
    except Exception as e:
        # Menangani jika terjadi sesuatu saat pembuatan topik baik ada topik sudah di buat atau kesalahan
        print(f"Terjadi sesuatu saat membuat topik '{kafka_topic}': {e}\n")

    # Loop untuk menghasilkan data transaksi palsu dan mengirimkannya ke Kafka setiap 3 detik
    while True:
        transaction_credit_card_data = TransactionDataGenerator.get_data()  # Menghasilkan data transaksi
        pay_load = json.dumps(transaction_credit_card_data).encode("utf-8")  # Mengubah data menjadi format JSON dan encode ke bytes
        key = transaction_credit_card_data['transaction_id'].encode("utf-8")  # Kunci sebagai bytes
        response = producer.send(topic=kafka_topic, value=pay_load, key= key)  # Mengirim data ke topik Kafka
        print(pay_load, flush=True)  # Menampilkan payload yang akan dikirim
        print("=-" * 63, flush=True)  # Separator untuk visualisasi
        print(response.get())  # Menampilkan respon dari Kafka
        print("=-" * 63, flush=True)  # Separator untuk visualisasi
        sleep(5)  # Menunggu selama 5 detik sebelum mengirim data berikutnya