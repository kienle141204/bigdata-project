import sqlite3
import os
from datetime import datetime
from loguru import logger

class MatchDB:
    """Simple SQLite database to track match scraping status."""
    
    def __init__(self, db_path: str = "ids.db"):
        self.db_path = db_path
        self._init_db()

    def _get_conn(self):
        return sqlite3.connect(self.db_path)

    def _init_db(self):
        """Initialize the matches table."""
        try:
            with self._get_conn() as conn:
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS matches (
                        match_id INTEGER PRIMARY KEY,
                        status TEXT,
                        last_updated TIMESTAMP
                    )
                """)
                conn.commit()
        except Exception as e:
            logger.error(f"Failed to init DB: {e}")

    def get_match_status(self, match_id: int) -> str:
        """
        Get the status of a match.
        Returns: 'PLAYED', 'SCHEDULED', or None if not found.
        """
        try:
            with self._get_conn() as conn:
                cursor = conn.execute("SELECT status FROM matches WHERE match_id = ?", (match_id,))
                result = cursor.fetchone()
                if result:
                    return result[0]
                return None
        except Exception as e:
            logger.error(f"Error checking status for {match_id}: {e}")
            return None

    def update_match_status(self, match_id: int, status: str):
        """Update or insert match status."""
        try:
            with self._get_conn() as conn:
                conn.execute("""
                    INSERT OR REPLACE INTO matches (match_id, status, last_updated)
                    VALUES (?, ?, ?)
                """, (match_id, status, datetime.now()))
                conn.commit()
        except Exception as e:
            logger.error(f"Error updating status for {match_id}: {e}")
