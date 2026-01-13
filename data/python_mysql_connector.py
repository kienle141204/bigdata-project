import mysql.connector
from mysql.connector import Error
from loguru import logger
from typing import Optional, List, Dict, Any
import pandas as pd
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.settings import MYSQL_CONFIG


class MySQLConnector:
    def __init__(self):
        self.connection = None
        self.config = MYSQL_CONFIG
        
    def connect(self) -> bool:
        try:
            connection_params = {
                "host": self.config["host"],
                "port": self.config["port"],
                "user": self.config["user"],
                "password": self.config["password"],
                "database": self.config["database"],
            }
            
            if self.config["ssl_mode"] == "REQUIRED":
                connection_params["ssl_disabled"] = False
                if self.config.get("ssl_ca"):
                    connection_params["ssl_ca"] = self.config["ssl_ca"]
            
            self.connection = mysql.connector.connect(**connection_params)
            
            if self.connection.is_connected():
                logger.info(f"Đã kết nối thành công với MySQL: {self.config['host']}:{self.config['port']}")
                return True
                
        except Error as e:
            logger.error(f"Lỗi khi kết nối MySQL: {e}")
            return False
            
        return False
    
    def disconnect(self):
        if self.connection and self.connection.is_connected():
            self.connection.close()
            logger.info("Đã ngắt kết nối MySQL")
    
    def execute_query(self, query: str) -> Optional[List[Dict[str, Any]]]:
        if not self.connection or not self.connection.is_connected():
            if not self.connect():
                return None
        
        try:
            cursor = self.connection.cursor(dictionary=True)
            cursor.execute(query)
            
            if query.strip().upper().startswith("SELECT"):
                results = cursor.fetchall()
                cursor.close()
                return results
            else:
                self.connection.commit()
                cursor.close()
                return []
                
        except Error as e:
            logger.error(f"Lỗi khi thực thi query: {e}")
            logger.error(f"Query: {query}")
            return None
    
    def execute_many(self, query: str, data: List[tuple]):
        if not self.connection or not self.connection.is_connected():
            if not self.connect():
                return False
        
        try:
            cursor = self.connection.cursor()
            cursor.executemany(query, data)
            self.connection.commit()
            cursor.close()
            logger.info(f"Đã thực thi {len(data)} câu lệnh")
            return True
            
        except Error as e:
            logger.error(f"Lỗi khi thực thi executemany: {e}")
            return False
    
    def read_to_dataframe(self, query: str) -> Optional[pd.DataFrame]:
        results = self.execute_query(query)
        if results:
            return pd.DataFrame(results)
        return None
    
    def write_dataframe(self, df: pd.DataFrame, table_name: str, if_exists: str = "append"):
        if not self.connection or not self.connection.is_connected():
            if not self.connect():
                return False
        
        try:
            from sqlalchemy import create_engine
            
            connection_string = (
                f"mysql+pymysql://{self.config['user']}:{self.config['password']}"
                f"@{self.config['host']}:{self.config['port']}/{self.config['database']}"
            )
            
            if self.config["ssl_mode"] == "REQUIRED":
                connection_string += "?ssl_disabled=False"
                if self.config.get("ssl_ca"):
                    connection_string += f"&ssl_ca={self.config['ssl_ca']}"
            
            engine = create_engine(connection_string)
            df.to_sql(table_name, engine, if_exists=if_exists, index=False)
            logger.info(f"Đã ghi {len(df)} dòng vào bảng {table_name}")
            return True
            
        except Exception as e:
            logger.error(f"Lỗi khi ghi DataFrame: {e}")
            return False
    
    def __enter__(self):
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.disconnect()


if __name__ == "__main__":
    with MySQLConnector() as mysql:
        result = mysql.execute_query("SHOW TABLES")
        if result:
            logger.info("Danh sách bảng:")
            for row in result:
                logger.info(f"  - {row}")



