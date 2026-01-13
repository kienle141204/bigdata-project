"""Database Setup - Initialize MySQL database schema."""
import os
import sys
from loguru import logger
from dotenv import load_dotenv

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from data.python_mysql_connector import MySQLConnector

load_dotenv()


def setup_database():
    """Tạo các bảng MySQL từ file schema.sql"""
    schema_file = os.path.join(os.path.dirname(__file__), "schema.sql")
    
    if not os.path.exists(schema_file):
        logger.error(f"File schema.sql không tồn tại: {schema_file}")
        return False
    
    logger.info("=" * 60)
    logger.info("SETUP MYSQL DATABASE SCHEMA")
    logger.info("=" * 60)
    
    with open(schema_file, "r", encoding="utf-8") as f:
        sql_content = f.read()
    
    with MySQLConnector() as mysql:
        if not mysql.connection or not mysql.connection.is_connected():
            logger.error("Không thể kết nối với MySQL")
            return False
        
        try:
            cursor = mysql.connection.cursor()
            
            statements = []
            current_statement = ""
            
            for line in sql_content.split('\n'):
                line = line.strip()
                if not line or line.startswith('--'):
                    continue
                
                current_statement += line + " "
                
                if line.endswith(';'):
                    statements.append(current_statement.strip())
                    current_statement = ""
            
            logger.info(f"Đã tìm thấy {len(statements)} câu lệnh SQL")
            
            for i, statement in enumerate(statements, 1):
                if not statement:
                    continue
                
                try:
                    logger.info(f"Đang thực thi câu lệnh {i}/{len(statements)}...")
                    cursor.execute(statement)
                    mysql.connection.commit()
                    logger.info(f"✓ Câu lệnh {i} đã được thực thi thành công")
                except Exception as e:
                    logger.warning(f"⚠ Câu lệnh {i} có lỗi (có thể đã tồn tại): {e}")
                    mysql.connection.rollback()
            
            cursor.close()
            
            logger.info("\n" + "=" * 60)
            logger.info("SETUP DATABASE HOÀN THÀNH!")
            logger.info("=" * 60)
            
            verify_tables(mysql)
            
            return True
            
        except Exception as e:
            logger.error(f"Lỗi khi setup database: {e}")
            logger.exception(e)
            return False


def verify_tables(mysql: MySQLConnector):
    """Kiểm tra các bảng đã được tạo"""
    logger.info("\n" + "=" * 60)
    logger.info("KIỂM TRA CÁC BẢNG ĐÃ TẠO")
    logger.info("=" * 60)
    
    tables_query = """
    SELECT TABLE_NAME, TABLE_ROWS 
    FROM information_schema.TABLES 
    WHERE TABLE_SCHEMA = DATABASE()
    AND TABLE_TYPE = 'BASE TABLE'
    AND TABLE_NAME NOT LIKE 'v_%'
    ORDER BY TABLE_NAME
    """
    
    results = mysql.execute_query(tables_query)
    
    if results:
        logger.info("\nCác bảng đã được tạo:")
        for row in results:
            table_name = row.get('TABLE_NAME', '')
            table_rows = row.get('TABLE_ROWS', 0)
            logger.info(f"  ✓ {table_name} ({table_rows} rows)")
    else:
        logger.warning("Không tìm thấy bảng nào!")
    
    views_query = """
    SELECT TABLE_NAME 
    FROM information_schema.VIEWS 
    WHERE TABLE_SCHEMA = DATABASE()
    AND TABLE_NAME LIKE 'v_%'
    ORDER BY TABLE_NAME
    """
    
    views = mysql.execute_query(views_query)
    if views:
        logger.info("\nCác views đã được tạo:")
        for row in views:
            view_name = row.get('TABLE_NAME', '')
            logger.info(f"  ✓ {view_name}")


if __name__ == "__main__":
    success = setup_database()
    sys.exit(0 if success else 1)

