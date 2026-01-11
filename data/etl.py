
"""ETL Pipeline: Bronze (JSON) -> Silver (CSV Tables)

Mục đích:
- Bronze: Lưu raw data dưới dạng JSON
- Silver: Tổng hợp thành 3 loại bảng CSV:
  1. Bảng vòng (Matchweek Table): match_id, matchweek
  2. Bảng mùa (Season Table): match_id, matchweek, season
  3. Bảng tổng hợp (Master Table): Tất cả các mùa
"""

import os
import json
import pandas as pd
from typing import List
from loguru import logger
from data.processor import S3DataStore

try:
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col
    HAS_SPARK = True
except ImportError:
    logger.warning("PySpark not found. Install it to use Spark features.")
    HAS_SPARK = False


class ETLPipeline:
    """Pipeline xử lý dữ liệu từ Bronze sang Silver."""
    
    def __init__(self, bucket_name: str = None, prefix: str = None):
        self.store = S3DataStore(bucket_name=bucket_name, prefix=prefix)
        self.spark = None
    
    def _init_spark(self):
        """Khởi tạo Spark Session."""
        if self.spark is None and HAS_SPARK:
            try:
                # Suppress Spark startup logs
                import logging
                logging.getLogger("py4j").setLevel(logging.ERROR)
                
                self.spark = SparkSession.builder \
                    .appName("PremierLeagueETL") \
                    .master("local[*]") \
                    .config("spark.driver.host", "127.0.0.1") \
                    .config("spark.ui.showConsoleProgress", "false") \
                    .config("spark.sql.adaptive.enabled", "false") \
                    .getOrCreate()
                
                # Set log level to ERROR only
                self.spark.sparkContext.setLogLevel("ERROR")
                
                logger.info("Spark session initialized")
            except Exception as e:
                logger.error(f"Failed to initialize Spark: {e}")
                self.spark = None
    
    def bronze_to_silver(self, seasons: List[str] = None):
        """
        Chuyển đổi Bronze (JSON) -> Silver (CSV Tables).
        
        Tạo 3 loại bảng:
        1. Bảng vòng: Theo từng mùa, chứa match_id và matchweek
        2. Bảng mùa: Theo từng mùa, chứa match_id, matchweek, season
        3. Bảng master: Tổng hợp tất cả các mùa
        
        Args:
            seasons: Danh sách mùa cần xử lý. Nếu None, tự động tìm tất cả mùa.
        """
        if not HAS_SPARK:
            logger.error("❌ PySpark is required. Please install pyspark.")
            return
        
        self._init_spark()
        if not self.spark:
            logger.error("❌ Cannot initialize Spark session.")
            return
        
        logger.info("="*60)
        logger.info("BRONZE -> SILVER: Creating Tables with Spark")
        logger.info("="*60)
        
        # Tự động tìm tất cả mùa nếu không chỉ định
        if seasons is None:
            logger.info("Discovering all seasons in Bronze layer...")
            seasons = self._discover_all_seasons()
            if not seasons:
                logger.warning("No seasons found in Bronze layer.")
                return
        
        logger.info(f"Processing {len(seasons)} seasons: {seasons}")
        
        # Lưu tất cả dữ liệu để tạo master table
        all_matches = []
        all_players = []
        all_events = []
        
        # Xử lý từng mùa
        for season in seasons:
            logger.info(f"\n{'='*60}")
            logger.info(f"Processing Season: {season}")
            logger.info(f"{'='*60}")
            
            # Đọc tất cả file JSON của mùa này
            matches = self._read_bronze_season(season)
            
            if not matches:
                logger.warning(f"No data found for season {season}")
                continue
            
            logger.info(f"Loaded {len(matches)} matches")
            
            # Thêm vào danh sách tổng hợp
            all_matches.extend(matches)
            
            # Convert to Pandas DataFrame first để clean data
            pdf = pd.DataFrame(matches)
            
            # Clean: Convert all columns to string để tránh mixed types
            # Chỉ giữ numeric columns như scores, matchweek
            numeric_cols = ['match_id', 'matchweek', 'home_score', 'away_score']
            for col in pdf.columns:
                if col in numeric_cols:
                    pdf[col] = pd.to_numeric(pdf[col], errors='coerce')
                else:
                    # Convert to string, replace None/NaN with empty string
                    pdf[col] = pdf[col].fillna('').astype(str)
            
            logger.info(f"Cleaned data: {len(pdf)} rows, {len(pdf.columns)} columns")
            
            # Tạo Spark DataFrame từ cleaned Pandas DataFrame
            df = self.spark.createDataFrame(pdf)
            
            # 1. Bảng vòng (Matchweek Table)
            self._create_matchweek_table(df, season)
            
            # 2. Bảng mùa (Season Table)
            self._create_season_table(df, season)
            
            # 3. Bảng cầu thủ (Players Table) - Riêng biệt
            players_data = []
            events_data = []
            
            # Đọc lại để extract players và events
            logger.info("Extracting players and events data...")
            for file_key in self.store.list_files(layer="bronze", season=season, ext="json"):
                if '/aggregates/' in file_key or '\\aggregates\\' in file_key:
                    continue
                
                try:
                    data = self.store.read_json(file_key)
                    if data and not isinstance(data, list):
                        players_data.extend(self._extract_players_from_json(data))
                        events_data.extend(self._extract_events_from_json(data))
                except:
                    pass
            
            if players_data:
                self._create_players_table(players_data, season)
                all_players.extend(players_data)
            
            # 4. Bảng sự kiện (Events Table) - Riêng biệt
            if events_data:
                self._create_events_table(events_data, season)
                all_events.extend(events_data)

        
        # 3. Bảng tổng hợp (Master Table)
        if all_matches:
            logger.info(f"\n{'='*60}")
            logger.info(f"Creating Master Table (All Seasons)")
            logger.info(f"{'='*60}")
            
            # Convert to Pandas và clean data
            pdf_master = pd.DataFrame(all_matches)
            
            # Clean data giống như trên
            numeric_cols = ['match_id', 'matchweek', 'home_score', 'away_score']
            for col in pdf_master.columns:
                if col in numeric_cols:
                    pdf_master[col] = pd.to_numeric(pdf_master[col], errors='coerce')
                else:
                    pdf_master[col] = pdf_master[col].fillna('').astype(str)
            
            logger.info(f"Cleaned master data: {len(pdf_master)} rows, {len(pdf_master.columns)} columns")
            
            # Tạo Spark DataFrame từ cleaned Pandas
            df_master = self.spark.createDataFrame(pdf_master)
            self._create_master_table(df_master)
        
        # 4. Master Players Table
        if all_players:
            logger.info(f"\n{'='*60}")
            logger.info(f"Creating Master Players Table (All Seasons)")
            logger.info(f"{'='*60}")
            
            pdf_players = pd.DataFrame(all_players)
            self._create_master_players_table(pdf_players)
        
        # 5. Master Events Table
        if all_events:
            logger.info(f"\n{'='*60}")
            logger.info(f"Creating Master Events Table (All Seasons)")
            logger.info(f"{'='*60}")
            
            pdf_events = pd.DataFrame(all_events)
            self._create_master_events_table(pdf_events)
        
        logger.info(f"\n{'='*60}")
        logger.info("BRONZE -> SILVER COMPLETE")
        logger.info(f"{'='*60}")
    
    def silver_to_gold(self):
        """
        Chuyển đổi Silver -> Gold: Tạo bảng riêng cho từng đội.
        
        Cấu trúc Gold:
        - gold/{team_name}/matches.csv
        - gold/{team_name}/players.csv
        - gold/{team_name}/events.csv
        """
        logger.info("="*60)
        logger.info("SILVER -> GOLD: Creating Team-Specific Tables")
        logger.info("="*60)
        
        # Đọc Master tables từ Silver
        logger.info("Reading Silver master tables...")
        
        try:
            # Read master matches
            master_matches_key = f"{self.store.prefix}/silver/master/MASTER_ALL_SEASONS.csv"
            matches_data = self.store.read_csv(master_matches_key)
            
            # Read master players
            master_players_key = f"{self.store.prefix}/silver/master/MASTER_PLAYERS_ALL_SEASONS.csv"
            players_data = self.store.read_csv(master_players_key)
            
            # Read master events
            master_events_key = f"{self.store.prefix}/silver/master/MASTER_EVENTS_ALL_SEASONS.csv"
            events_data = self.store.read_csv(master_events_key)
            
        except Exception as e:
            logger.error(f"Error reading Silver master tables: {e}")
            return
        
        # Convert to DataFrame (read_csv may return list of dicts or None)
        if matches_data is None:
            logger.warning("No matches data found in Silver layer")
            return
        
        # Simple conversion - check type first
        if isinstance(matches_data, pd.DataFrame):
            df_matches = matches_data
        else:
            df_matches = pd.DataFrame(matches_data)
        
        if isinstance(players_data, pd.DataFrame):
            df_players = players_data
        elif players_data is not None:
            df_players = pd.DataFrame(players_data)
        else:
            df_players = pd.DataFrame()
        
        if isinstance(events_data, pd.DataFrame):
            df_events = events_data
        elif events_data is not None:
            df_events = pd.DataFrame(events_data)
        else:
            df_events = pd.DataFrame()
        
        if df_matches.empty:
            logger.warning("No matches data in Silver layer")
            return
        
        # Discover all teams
        teams = set()
        if 'home_team' in df_matches.columns:
            teams.update(df_matches['home_team'].unique())
        if 'away_team' in df_matches.columns:
            teams.update(df_matches['away_team'].unique())
        
        # Remove empty/null teams
        teams = {t for t in teams if t and str(t).strip() and str(t) != 'nan'}
        teams = sorted(list(teams))
        
        logger.info(f"Found {len(teams)} teams")
        logger.info(f"Teams: {', '.join(teams[:5])}..." if len(teams) > 5 else f"Teams: {', '.join(teams)}")
        
        # Process each team
        for team in teams:
            logger.info(f"\nProcessing team: {team}")
            self._create_team_gold_tables(team, df_matches, df_players, df_events)
        
        logger.info(f"\n{'='*60}")
        logger.info("SILVER -> GOLD COMPLETE")
        logger.info(f"{'='*60}")
    
    def _create_team_gold_tables(self, team: str, df_matches: pd.DataFrame, 
                                  df_players: pd.DataFrame, df_events: pd.DataFrame):
        """Create Gold tables for a specific team."""
        # Clean team name for folder
        team_folder = team.replace(" ", "_").replace("/", "_")
        
        # 1. Team Matches - Filter matches where team is home or away
        team_matches = df_matches[
            (df_matches['home_team'] == team) | (df_matches['away_team'] == team)
        ].copy()
        
        if not team_matches.empty:
            s3_key = f"{self.store.prefix}/gold/{team_folder}/matches.csv"
            self.store.upload_csv(team_matches.to_dict('records'), layer="gold", s3_key=s3_key)
            logger.info(f"  Matches: {len(team_matches)} rows -> {s3_key}")
        
        # 2. Team Players
        if not df_players.empty and 'team' in df_players.columns:
            team_players = df_players[df_players['team'] == team].copy()
            
            if not team_players.empty:
                s3_key = f"{self.store.prefix}/gold/{team_folder}/players.csv"
                self.store.upload_csv(team_players.to_dict('records'), layer="gold", s3_key=s3_key)
                logger.info(f"  Players: {len(team_players)} rows -> {s3_key}")
        
        # 3. Team Events
        if not df_events.empty and 'team' in df_events.columns:
            team_events = df_events[df_events['team'] == team].copy()
            
            if not team_events.empty:
                s3_key = f"{self.store.prefix}/gold/{team_folder}/events.csv"
                self.store.upload_csv(team_events.to_dict('records'), layer="gold", s3_key=s3_key)
                logger.info(f"  Events: {len(team_events)} rows -> {s3_key}")
    
    def _read_bronze_season(self, season: str) -> List[dict]:
        """
        Đọc tất cả file JSON của một mùa từ Bronze layer và flatten toàn bộ thông tin.
        
        Returns:
            List of flattened dicts with all match information and statistics
        """
        files = self.store.list_files(layer="bronze", season=season, ext="json")
        logger.info(f"Found {len(files)} JSON files")
        
        matches = []
        skipped = 0
        
        for file_key in files:
            # Bỏ qua folder aggregates (chứa processed data, không phải raw)
            if '/aggregates/' in file_key or '\\aggregates\\' in file_key:
                skipped += 1
                continue
            
            try:
                data = self.store.read_json(file_key)
                if not data:
                    continue
                
                # Handle nếu data là list (aggregate files)
                if isinstance(data, list):
                    skipped += 1
                    continue
                
                # Flatten toàn bộ thông tin từ JSON
                flattened = self._flatten_match_data(data)
                
                if flattened:
                    matches.append(flattened)
                    
            except Exception as e:
                logger.debug(f"Skip {file_key}: {e}")
                skipped += 1
                continue
        
        if skipped > 0:
            logger.info(f"Skipped {skipped} non-raw files (aggregates/processed)")
        
        return matches
    
    def _flatten_match_data(self, data: dict) -> dict:
        """
        Flatten toàn bộ thông tin từ JSON match data.
        
        Bao gồm:
        - match_id, season, matchweek
        - match_info: teams, scores (BỎ date, venue, referee, kickoff, stadium, attendance)
        - lineups: danh sách cầu thủ (starting XI và substitutes)
        - statistics: tất cả stats (flatten home_ và away_)
        
        Returns:
            Flattened dict hoặc None nếu thiếu thông tin cơ bản
        """
        # Basic info
        match_id = data.get("match_id")
        matchweek = data.get("matchweek")
        season = data.get("season")
        
        if not (match_id and matchweek and season):
            return None
        
        # Start with basic fields
        flat = {
            "match_id": match_id,
            "season": season,
            "matchweek": matchweek,
            "url": data.get("url"),
            "scraped_at": data.get("scraped_at")
        }
        
        # Match info (CHỈ LẤY teams và scores, BỎ date, venue, referee, etc.)
        match_info = data.get("match_info", {})
        if match_info:
            flat["home_team"] = match_info.get("home_team")
            flat["away_team"] = match_info.get("away_team")
            flat["home_score"] = match_info.get("home_score")
            flat["away_score"] = match_info.get("away_score")
        
        # Lineups - Extract player names
        lineups = data.get("lineups", {})
        if lineups:
            # Home team lineups
            home_lineup = lineups.get("home", {})
            if home_lineup:
                flat["home_formation"] = home_lineup.get("formation")
                flat["home_manager"] = home_lineup.get("manager")
                
                # Starting XI - extract player names
                home_starting = home_lineup.get("starting_xi", [])
                if home_starting:
                    players = []
                    for player in home_starting:
                        if isinstance(player, dict):
                            name = player.get("name", "")
                            # Clean name (remove number prefix like "22Mignolet" -> "Mignolet")
                            name = ''.join([c for c in name if not c.isdigit()]).strip()
                            if name:
                                players.append(name)
                    flat["home_starting_xi"] = ", ".join(players) if players else ""
                
                # Substitutes
                home_subs = home_lineup.get("substitutes", [])
                if home_subs:
                    subs = []
                    for player in home_subs:
                        if isinstance(player, dict):
                            name = player.get("name", "").split("\n")[0]  # Take first part before \n
                            name = ''.join([c for c in name if not c.isdigit()]).strip()
                            if name:
                                subs.append(name)
                    flat["home_substitutes"] = ", ".join(subs) if subs else ""
            
            # Away team lineups
            away_lineup = lineups.get("away", {})
            if away_lineup:
                flat["away_formation"] = away_lineup.get("formation")
                flat["away_manager"] = away_lineup.get("manager")
                
                # Starting XI
                away_starting = away_lineup.get("starting_xi", [])
                if away_starting:
                    players = []
                    for player in away_starting:
                        if isinstance(player, dict):
                            name = player.get("name", "")
                            name = ''.join([c for c in name if not c.isdigit()]).strip()
                            if name:
                                players.append(name)
                    flat["away_starting_xi"] = ", ".join(players) if players else ""
                
                # Substitutes
                away_subs = away_lineup.get("substitutes", [])
                if away_subs:
                    subs = []
                    for player in away_subs:
                        if isinstance(player, dict):
                            name = player.get("name", "").split("\n")[0]
                            name = ''.join([c for c in name if not c.isdigit()]).strip()
                            if name:
                                subs.append(name)
                    flat["away_substitutes"] = ", ".join(subs) if subs else ""
        
        # Events - Goals, scorers, assists
        events = data.get("events", {})
        if events:
            # Home goals
            home_goals = events.get("home_goals", [])
            if home_goals:
                scorers = []
                assists = []
                for goal in home_goals:
                    if isinstance(goal, dict):
                        scorer = goal.get("scorer", "")
                        if scorer:
                            scorers.append(scorer.strip())
                        
                        assist = goal.get("assist")
                        if assist and assist != "None" and assist != "null":
                            assists.append(assist.strip())
                
                flat["home_goal_scorers"] = ", ".join(scorers) if scorers else ""
                flat["home_goal_assists"] = ", ".join(assists) if assists else ""
            else:
                flat["home_goal_scorers"] = ""
                flat["home_goal_assists"] = ""
            
            # Away goals
            away_goals = events.get("away_goals", [])
            if away_goals:
                scorers = []
                assists = []
                for goal in away_goals:
                    if isinstance(goal, dict):
                        scorer = goal.get("scorer", "")
                        if scorer:
                            scorers.append(scorer.strip())
                        
                        assist = goal.get("assist")
                        if assist and assist != "None" and assist != "null":
                            assists.append(assist.strip())
                
                flat["away_goal_scorers"] = ", ".join(scorers) if scorers else ""
                flat["away_goal_assists"] = ", ".join(assists) if assists else ""
            else:
                flat["away_goal_scorers"] = ""
                flat["away_goal_assists"] = ""
        
        # Statistics - flatten home_ and away_ prefixes
        statistics = data.get("statistics", {})
        if statistics:
            for stat_name, stat_values in statistics.items():
                if not isinstance(stat_values, dict):
                    continue
                
                # Clean stat name (lowercase, remove special chars)
                clean_name = stat_name.lower() \
                    .replace(" ", "_") \
                    .replace("(", "") \
                    .replace(")", "") \
                    .replace("%", "pct") \
                    .strip("_")
                
                # Home value
                home_val = stat_values.get("home_parsed") or stat_values.get("home")
                if home_val is not None:
                    flat[f"home_{clean_name}"] = home_val
                
                # Away value
                away_val = stat_values.get("away_parsed") or stat_values.get("away")
                if away_val is not None:
                    flat[f"away_{clean_name}"] = away_val
        
        return flat
    
    def _create_matchweek_table(self, df, season: str):
        """Tạo bảng vòng: Tất cả thông tin, group by matchweek"""
        # Remove duplicates và sort
        df_matchweek = df.distinct().orderBy("matchweek", "match_id")
        
        # Convert to Pandas
        pdf = df_matchweek.toPandas()
        
        # Upload to S3
        s3_key = f"{self.store.prefix}/silver/{season.replace('/', '-')}/matchweek_table.csv"
        
        self.store.upload_csv(pdf.to_dict('records'), layer="silver", s3_key=s3_key)
        logger.info(f"Matchweek Table: {s3_key}")
        logger.info(f"  Rows: {len(pdf)}, Columns: {len(pdf.columns)}")
    
    def _create_season_table(self, df, season: str):
        """Tạo bảng mùa: Tất cả thông tin của mùa này"""
        # Remove duplicates và sort
        df_season = df.distinct().orderBy("matchweek", "match_id")
        
        # Convert to Pandas
        pdf = df_season.toPandas()
        
        # Upload to S3
        s3_key = f"{self.store.prefix}/silver/{season.replace('/', '-')}/season_table.csv"
        
        self.store.upload_csv(pdf.to_dict('records'), layer="silver", s3_key=s3_key)
        logger.info(f"Season Table: {s3_key}")
        logger.info(f"  Rows: {len(pdf)}, Columns: {len(pdf.columns)}")
    
    def _create_master_table(self, df):
        """Tạo bảng tổng hợp: Tất cả thông tin từ tất cả các mùa"""
        # Remove duplicates và sort
        df_master = df.distinct().orderBy("season", "matchweek", "match_id")
        
        # Convert to Pandas
        pdf = df_master.toPandas()
        
        # Upload to S3
        s3_key = f"{self.store.prefix}/silver/master/MASTER_ALL_SEASONS.csv"
        
        self.store.upload_csv(pdf.to_dict('records'), layer="silver", s3_key=s3_key)
        logger.info(f"Master Table: {s3_key}")
        logger.info(f"  Total Rows: {len(pdf)}, Columns: {len(pdf.columns)}")
        logger.info(f"  Seasons: {pdf['season'].nunique() if 'season' in pdf.columns else 0}")
        logger.info(f"  Matches: {len(pdf)}")
    
    def _extract_players_data(self, matches: List[dict], season: str) -> List[dict]:
        """
        Extract danh sách cầu thủ từ raw match data.
        
        Returns list of dicts with: match_id, season, matchweek, team, player_name, position
        """
        players = []
        
        for match in matches:
            try:
                # Lấy từ data gốc thay vì flattened
                data_key = "raw_data"  # Cần lưu raw data
                # Vì matches đã flatten, cần đọc lại từ JSON
                # Tạm thời skip, sẽ đọc trực tiếp từ Bronze
            except:
                pass
        
        return players
    
    def _extract_players_from_json(self, data: dict) -> List[dict]:
        """Extract players from a single match JSON."""
        players = []
        
        match_id = data.get("match_id")
        season = data.get("season")
        matchweek = data.get("matchweek")
        
        if not (match_id and season and matchweek):
            return players
        
        lineups = data.get("lineups", {})
        
        # Home team
        home_team = data.get("match_info", {}).get("home_team")
        home_lineup = lineups.get("home", {})
        
        # Starting XI
        for player in home_lineup.get("starting_xi", []):
            if isinstance(player, dict):
                name = player.get("name", "")
                # Clean name
                name = ''.join([c for c in name if not c.isdigit()]).strip()
                if name:
                    players.append({
                        "match_id": match_id,
                        "season": season,
                        "matchweek": matchweek,
                        "team": home_team,
                        "player_name": name,
                        "position": "Starting XI"
                    })
        
        # Substitutes
        for player in home_lineup.get("substitutes", []):
            if isinstance(player, dict):
                name = player.get("name", "").split("\n")[0]
                name = ''.join([c for c in name if not c.isdigit()]).strip()
                if name:
                    players.append({
                        "match_id": match_id,
                        "season": season,
                        "matchweek": matchweek,
                        "team": home_team,
                        "player_name": name,
                        "position": "Substitute"
                    })
        
        # Away team
        away_team = data.get("match_info", {}).get("away_team")
        away_lineup = lineups.get("away", {})
        
        # Starting XI
        for player in away_lineup.get("starting_xi", []):
            if isinstance(player, dict):
                name = player.get("name", "")
                name = ''.join([c for c in name if not c.isdigit()]).strip()
                if name:
                    players.append({
                        "match_id": match_id,
                        "season": season,
                        "matchweek": matchweek,
                        "team": away_team,
                        "player_name": name,
                        "position": "Starting XI"
                    })
        
        # Substitutes
        for player in away_lineup.get("substitutes", []):
            if isinstance(player, dict):
                name = player.get("name", "").split("\n")[0]
                name = ''.join([c for c in name if not c.isdigit()]).strip()
                if name:
                    players.append({
                        "match_id": match_id,
                        "season": season,
                        "matchweek": matchweek,
                        "team": away_team,
                        "player_name": name,
                        "position": "Substitute"
                    })
        
        return players
    
    def _extract_events_data(self, matches: List[dict], season: str) -> List[dict]:
        """Extract events data - will be implemented."""
        return []
    
    def _extract_events_from_json(self, data: dict) -> List[dict]:
        """Extract events (goals, cards) from a single match JSON."""
        events = []
        
        match_id = data.get("match_id")
        season = data.get("season")
        matchweek = data.get("matchweek")
        
        if not (match_id and season and matchweek):
            return events
        
        match_events = data.get("events", {})
        home_team = data.get("match_info", {}).get("home_team")
        away_team = data.get("match_info", {}).get("away_team")
        
        def clean_player_and_minute(text):
            """Extract player name and minute from text like 'Taylor 62'"""
            if not text:
                return "", ""
            
            text = str(text).strip()
            minute = ""
            player_name = text
            
            # Check if has minute (ends with ')
            if "'" in text:
                # Split by ' to get minute part
                parts = text.split("'")[0].strip().split()
                if parts:
                    # Last part before ' should be minute
                    last_part = parts[-1]
                    if last_part.isdigit():
                        minute = last_part
                        # Remove minute from player name
                        player_name = " ".join(parts[:-1]).strip()
            
            return player_name, minute
        
        # Home goals
        for goal in match_events.get("home_goals", []):
            if isinstance(goal, dict):
                scorer = goal.get("scorer", "")
                player_name, minute = clean_player_and_minute(scorer)
                
                if player_name:
                    events.append({
                        "match_id": match_id,
                        "season": season,
                        "matchweek": matchweek,
                        "team": home_team,
                        "event_type": "Goal",
                        "player": player_name,
                        "minute": minute
                    })
        
        # Away goals
        for goal in match_events.get("away_goals", []):
            if isinstance(goal, dict):
                scorer = goal.get("scorer", "")
                player_name, minute = clean_player_and_minute(scorer)
                
                if player_name:
                    events.append({
                        "match_id": match_id,
                        "season": season,
                        "matchweek": matchweek,
                        "team": away_team,
                        "event_type": "Goal",
                        "player": player_name,
                        "minute": minute
                    })
        
        # Yellow cards
        for card in match_events.get("home_yellow_cards", []):
            if card:
                player_name, minute = clean_player_and_minute(card)
                if player_name:
                    events.append({
                        "match_id": match_id,
                        "season": season,
                        "matchweek": matchweek,
                        "team": home_team,
                        "event_type": "Yellow Card",
                        "player": player_name,
                        "minute": minute
                    })
        
        for card in match_events.get("away_yellow_cards", []):
            if card:
                player_name, minute = clean_player_and_minute(card)
                if player_name:
                    events.append({
                        "match_id": match_id,
                        "season": season,
                        "matchweek": matchweek,
                        "team": away_team,
                        "event_type": "Yellow Card",
                        "player": player_name,
                        "minute": minute
                    })
        
        # Red cards
        for card in match_events.get("home_red_cards", []):
            if card:
                player_name, minute = clean_player_and_minute(card)
                if player_name:
                    events.append({
                        "match_id": match_id,
                        "season": season,
                        "matchweek": matchweek,
                        "team": home_team,
                        "event_type": "Red Card",
                        "player": player_name,
                        "minute": minute
                    })
        
        for card in match_events.get("away_red_cards", []):
            if card:
                player_name, minute = clean_player_and_minute(card)
                if player_name:
                    events.append({
                        "match_id": match_id,
                        "season": season,
                        "matchweek": matchweek,
                        "team": away_team,
                        "event_type": "Red Card",
                        "player": player_name,
                        "minute": minute
                    })
        
        return events
    
    def _create_players_table(self, players_data: List[dict], season: str):
        """Create Players Table CSV."""
        if not players_data:
            return
        
        pdf = pd.DataFrame(players_data)
        s3_key = f"{self.store.prefix}/silver/{season.replace('/', '-')}/players_table.csv"
        
        self.store.upload_csv(pdf.to_dict('records'), layer="silver", s3_key=s3_key)
        logger.info(f"Players Table: {s3_key}")
        logger.info(f"  Rows: {len(pdf)}")
    
    def _create_events_table(self, events_data: List[dict], season: str):
        """Create Events Table CSV."""
        if not events_data:
            return
        
        pdf = pd.DataFrame(events_data)
        s3_key = f"{self.store.prefix}/silver/{season.replace('/', '-')}/events_table.csv"
        
        self.store.upload_csv(pdf.to_dict('records'), layer="silver", s3_key=s3_key)
        logger.info(f"Events Table: {s3_key}")
        logger.info(f"  Rows: {len(pdf)}")
    
    def _create_master_players_table(self, pdf: pd.DataFrame):
        """Create Master Players Table CSV (all seasons)."""
        if pdf.empty:
            return
        
        # Remove duplicates if any
        pdf = pdf.drop_duplicates()
        pdf = pdf.sort_values(by=["season", "matchweek", "team", "player_name"])
        
        s3_key = f"{self.store.prefix}/silver/master/MASTER_PLAYERS_ALL_SEASONS.csv"
        
        self.store.upload_csv(pdf.to_dict('records'), layer="silver", s3_key=s3_key)
        logger.info(f"Master Players Table: {s3_key}")
        logger.info(f"  Total Rows: {len(pdf)}")
        logger.info(f"  Seasons: {pdf['season'].nunique() if 'season' in pdf.columns else 0}")
    
    def _create_master_events_table(self, pdf: pd.DataFrame):
        """Create Master Events Table CSV (all seasons)."""
        if pdf.empty:
            return
        
        # Remove duplicates if any
        pdf = pdf.drop_duplicates()
        pdf = pdf.sort_values(by=["season", "matchweek", "team", "event_type"])
        
        s3_key = f"{self.store.prefix}/silver/master/MASTER_EVENTS_ALL_SEASONS.csv"
        
        self.store.upload_csv(pdf.to_dict('records'), layer="silver", s3_key=s3_key)
        logger.info(f"Master Events Table: {s3_key}")
        logger.info(f"  Total Rows: {len(pdf)}")
        logger.info(f"  Seasons: {pdf['season'].nunique() if 'season' in pdf.columns else 0}")
        logger.info(f"  Event Types: {pdf['event_type'].nunique() if 'event_type' in pdf.columns else 0}")

    
    def _discover_all_seasons(self) -> List[str]:
        """Tự động tìm tất cả các mùa trong Bronze layer."""
        try:
            prefix_path = f"{self.store.prefix}/bronze/"
            response = self.store.s3_client.list_objects_v2(
                Bucket=self.store.bucket_name,
                Prefix=prefix_path,
                Delimiter='/'
            )
            
            seasons = set()
            if 'CommonPrefixes' in response:
                for prefix in response['CommonPrefixes']:
                    # Extract season từ path như "bronze/2011-12/"
                    season_part = prefix['Prefix'].replace(prefix_path, '').strip('/')
                    # Convert "2011-12" -> "2011/12"
                    if '-' in season_part and len(season_part) == 7:
                        seasons.add(season_part.replace('-', '/'))
            
            return sorted(list(seasons))
        except Exception as e:
            logger.error(f"Error discovering seasons: {e}")
            return []
