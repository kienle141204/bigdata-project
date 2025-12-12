import os
import sys
import json
import time
from datetime import datetime
from threading import Timer
from pathlib import Path
from dotenv import load_dotenv
from kafka import KafkaConsumer

# Add project root to path
sys.path.append(str(Path(__file__).resolve().parent.parent))

from data_lake import S3DataLakeClient

# Load env in case configured there
load_dotenv()

class S3IngestionConsumer:
    """
    Kafka Consumer: Đọc dữ liệu từ Kafka va batch upload lên S3 Data Lake
    """
    def __init__(self, topic, batch_size=50, batch_timeout=60):
        self.topic = topic
        self.batch_size = batch_size
        self.batch_timeout = batch_timeout # seconds
        
        # Buffer chứa data
        self.buffer = []
        self.last_flush_time = time.time()
        
        # S3 Client
        self.s3 = S3DataLakeClient()
        
        # Kafka setup
        bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
        self.consumer = KafkaConsumer(
            self.topic,
            bootstrap_servers=bootstrap_servers,
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id='datalake_archiver_group',
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
        print(f"✅ S3 Ingestion Consumer started on topic: {self.topic}")

    def flush_buffer(self):
        """Ghi buffer lên S3"""
        if not self.buffer:
            return

        # Tạo tên file: topic_YYYYMMDD_HHMMSS.json
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{self.topic}_{timestamp}.json"
        
        print(f"📦 Flushing {len(self.buffer)} items -> {filename}")
        
        try:
            # Ghi file tạm
            local_path = f"logs/{filename}"
            with open(local_path, "w", encoding="utf-8") as f:
                json.dump(self.buffer, f, ensure_ascii=False, indent=2)
            
            # Upload lên S3 (Raw layer)
            success = self.s3.upload_to_raw(local_path, source="kafka_ingestion")
            
            if success:
                # Xóa file tạm & clear buffer
                if os.path.exists(local_path):
                    os.remove(local_path)
                self.buffer = []
                self.last_flush_time = time.time()
                print("✨ Batch upload successful!")
            else:
                print("❌ Upload failed, keeping data in buffer retry next time.")
                
        except Exception as e:
            print(f"❌ Error during flush: {e}")

    def start(self):
        """Vòng lặp chính"""
        print("🎧 Waiting for messages...")
        try:
            for message in self.consumer:
                self.buffer.append(message.value)
                
                # Check điều kiện flush: Đủ số lượng hoặc quá thời gian
                current_time = time.time()
                is_full = len(self.buffer) >= self.batch_size
                is_timeout = (current_time - self.last_flush_time) >= self.batch_timeout

                if is_full or is_timeout:
                    self.flush_buffer()
                    
        except KeyboardInterrupt:
            print("🛑 Stopping consumer...")
            self.flush_buffer() # Flush nốt dữ liệu còn lại
            self.consumer.close()

if __name__ == "__main__":
    # Đọc topic raw_news_data
    consumer = S3IngestionConsumer(topic="raw_news_data")
    consumer.start()
