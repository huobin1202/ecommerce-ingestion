# Hướng dẫn chạy
# Bước 1 — Chuẩn bị môi trường
Windows
Docker Desktop
Python
VS Code

# Windows Powershell
Kiểm tra Python: python --version
Kiểm tra Docker: docker --version
docker compose version

# Bước 2 — Chạy Apache Kafka bằng Docker
cd D:\BigData\ecommerce-ingestion sau đó: docker compose up -d
Kiểm tra: docker ps
Cần thấy: ecommmer-event, trạng thái UP

# Bước 3 - Tạo Kafka Topic
cd D:\BigData\ecommerce-ingestion sau đó:
docker exec ecommerce-kafka /opt/kafka/bin/kafka-topics.sh --create --topic ecommerce-events --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
Kiểm tra: docker exec ecommerce-kafka /opt/kafka/bin/kafka-topics.sh --list --bootstrap-server localhost:9092

# Bước 4 - Tạo Python environment
cd D:\BigData\ecommerce-ingestion sau đó: venv\Scripts\activate
Cài thư viện: pip install kafka-python
Lưu dependency: pip freeze > requirements.txt ( nếu cần )

# Bước 5 - Chạy Producer
cd D:\BigData\ecommerce-ingestion
python producer/producer.py

# Bước 6 - Chạy Consumer
cd D:\BigData\ecommerce-ingestion
consumer/consumer.py

# Chạy song song 
chạy song song Producer/Consumer để kiểm tra

# Bước 7 - Chạy Generator có giới hạn số lượng
cd D:\BigData\ecommerce-ingestion
benchmark/benchmark.py