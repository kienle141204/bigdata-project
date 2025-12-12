import os
import sys
import json
from pathlib import Path
from dotenv import load_dotenv

# Thêm thư mục gốc vào đường dẫn để import được module data_lake
project_root = Path(__file__).resolve().parent.parent
sys.path.append(str(project_root))

from data_lake import S3DataLakeClient

# Load biến môi trường từ file .env
load_dotenv()

def test_s3_connection():
    print("🚀 Bắt đầu kiểm tra kết nối Data Lake (S3)...")
    print("-" * 50)
    
    # 1. Kiểm tra biến môi trường
    bucket_name = os.getenv("AWS_S3_BUCKET")
    print(f"📋 Cấu hình:")
    print(f"   - Bucket: {bucket_name}")
    print(f"   - Region: {os.getenv('AWS_REGION')}")
    
    if not os.getenv("AWS_ACCESS_KEY_ID"):
        print("❌ LỖI: Chưa tìm thấy AWS_ACCESS_KEY_ID. Hãy kiểm tra file .env")
        return

    try:
        # 2. Khởi tạo Client
        print("\n1️⃣  Đang khởi tạo S3 Client...")
        client = S3DataLakeClient()
        print("✅ Kết nối client thành công!")

        # 3. Tạo dữ liệu test
        test_filename = "connection_test.json"
        test_data = {
            "message": "Hello Big Data!", 
            "timestamp": "test_time",
            "status": "connection_ok"
        }
        
        # Ghi tạm ra file local (để test hàm upload_to_raw dùng file path)
        with open(test_filename, "w") as f:
            json.dump(test_data, f)
        
        # 4. Test Upload
        print("\n2️⃣  Đang test Upload lên lớp Raw...")
        source_name = "test_connection"
        # Upload file đó lên S3
        success = client.upload_to_raw(test_filename, source=source_name)
        
        if success:
            print("✅ Upload thành công!")
        else:
            print("❌ Upload thất bại! Kiểm tra lại quyền hoặc mạng.")
            return

        # 5. Test List Files
        print("\n3️⃣  Đang kiểm tra file trên S3...")
        files = client.list_raw_files(source=source_name)
        print(f"   Tìm thấy {len(files)} file trong thư mục raw/{source_name}/:")
        found_our_file = False
        for f in files:
            print(f"   - {f}")
            if test_filename in f:
                found_our_file = True

        if found_our_file:
            print("✅ Đã tìm thấy file vừa upload trên S3. Kết nối OK!")
        else:
            print("⚠️ Cảnh báo: Upload báo thành công nhưng không tìm thấy file trong list.")

        # 6. Dọn dẹp (Xóa file test trên S3)
        print("\n4️⃣  Đang dọn dẹp file test...")
        # Tìm lại đúng key của file vừa up để xóa (vì nó có partition ngày tháng)
        for f in files:
            if test_filename in f:
                client.delete_object(f)
                
    except Exception as e:
        print(f"\n❌ CÓ LỖI XẢY RA: {e}")
        import traceback
        traceback.print_exc()
        
    finally:
        # Xóa file local
        if os.path.exists(test_filename):
            os.remove(test_filename)
            print("\n🧹 Đã xóa file test local.")

if __name__ == "__main__":
    test_s3_connection()
