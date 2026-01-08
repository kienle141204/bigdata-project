
"""ETL Pipeline for processing Premier League data through Bronze, Silver, and Gold layers."""
import pandas as pd
from typing import Dict, Any, List
from loguru import logger
from data.processor import S3DataStore

class ETLPipeline:
    def __init__(self, bucket_name: str = None, prefix: str = None):
        self.store = S3DataStore(bucket_name=bucket_name, prefix=prefix)

    def process_bronze_to_silver(self, season: str, matchweek: int = None):
        """Clean raw data from Bronze and save to Silver as CSV (Individual + Aggregate)."""
        logger.info(f"Starting Bronze -> Silver ETL for Season {season}, MW {matchweek}")
        
        files = self.store.list_files(layer="bronze", season=season, matchweek=matchweek, ext="json")
        logger.info(f"Found {len(files)} files in Bronze layer")
        
        processed_count = 0
        silver_dataset = []

        for file_key in files:
            data = self.store.read_json(file_key)
            if not data:
                continue
                
            # 1. Clean
            cleaned_data = self._clean_match_data(data)
            
            # 2. Flatten (Wide Format) for Silver CSV
            silver_record = self._flatten_match_data(cleaned_data)
            silver_dataset.append(silver_record)
            
            # 3. Save to Silver as Individual CSV
            self.store.upload_csv(silver_record, layer="silver")
            processed_count += 1
            
        # 4. Save Silver Aggregate CSV
        if silver_dataset:
            timestamp = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
            agg_key = f"{self.store.prefix}/silver/{season.replace('/', '-')}/aggregates/mw{matchweek:02d}_full_stats_{timestamp}.csv"
            self.store.upload_csv(silver_dataset, layer="silver", s3_key=agg_key)

        logger.info(f"Bronze -> Silver complete. Processed {processed_count} files. Aggregate saved.")

    def process_silver_to_gold(self, season: str, matchweek: int = None):
        """Read Silver CSVs, filter for Man Utd, and save to Gold as CSV."""
        logger.info(f"Starting Silver -> Gold ETL for Season {season}, MW {matchweek} (Target: Man Utd)")
        
        # List CSV files in Silver
        files = self.store.list_files(layer="silver", season=season, matchweek=matchweek, ext="csv")
        logger.info(f"Found {len(files)} files in Silver layer")
        
        gold_dataset = []
        target_aliases = ["Man Utd", "Manchester United"]
        
        for file_key in files:
            df = self.store.read_csv(file_key)
            if df is None or df.empty:
                continue
            
            # Convert single-row DF back to dict for easy processing
            row = df.iloc[0].to_dict()
            
            # Transformation Logic - Filter for MU
            # We reuse the logic but adapted for flat input
            gold_record = self._transform_flat_data_for_team(row, target_aliases)
            
            if gold_record:
                gold_dataset.append(gold_record)

        # Save aggregated Gold dataset as single CSV
        if gold_dataset:
            timestamp = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
            agg_key = f"{self.store.prefix}/gold/{season.replace('/', '-')}/analytics/mu_stats_mw{matchweek}_{timestamp}.csv"
            self.store.upload_csv(gold_dataset, layer="gold", s3_key=agg_key)
            
        logger.info(f"Silver -> Gold complete. Processed {len(gold_dataset)} records for Man Utd.")

    def create_global_aggregates(self, seasons: List[str]):
        """Consolidate ALL data from processed seasons into Master CSVs for Silver and Gold."""
        logger.info("="*30)
        logger.info("CREATING GLOBAL MASTER DATASETS")
        logger.info("="*30)
        
        # 1. Silver Master (All Teams, All Seasons)
        silver_dfs = []
        for season in seasons:
            # Find all aggregate files in the 'aggregates' folder for this season
            agg_files = self.store.list_aggregates(layer="silver", season=season)
            
            # Logic to pick only latest per matchweek? 
            # Simplified: Read all, then drop duplicates based on match_id
            for f in agg_files:
                df = self.store.read_csv(f)
                if df is not None and not df.empty:
                    silver_dfs.append(df)
        
        if silver_dfs:
            silver_master = pd.concat(silver_dfs, ignore_index=True)
            # Dedup: If we ran pipeline multiple times, we might have multiple CSVs covering same matches
            if "match_id" in silver_master.columns:
                silver_master = silver_master.drop_duplicates(subset=["match_id"], keep="last")
            
            # Sort
            if "season" in silver_master.columns and "matchweek" in silver_master.columns:
                silver_master = silver_master.sort_values(by=["season", "matchweek"])

            timestamp = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
            silver_key = f"{self.store.prefix}/silver/master/MASTER_ALL_SEASONS_{timestamp}.csv"
            self.store.upload_csv(silver_master.to_dict('records'), layer="silver", s3_key=silver_key) 
            # Note: upload_csv handles list of dicts. or if we pass dataframe directly?
            # Existing upload_csv takes Dict or List[Dict]. Let's pass List[Dict].
            
            logger.info(f"SILVER MASTER UPDATED: {len(silver_master)} records across {len(seasons)} seasons")

        # 2. Gold Master (Man Utd, All Seasons)
        gold_dfs = []
        for season in seasons:
            # Gold currently stores aggregated MU stats in 'analytics' folder usually, 
            # or 'aggregates' if we changed it? 
            # In process_silver_to_gold: prefix/gold/{season}/analytics/mu_stats_mw...
            # This is slightly different path than 'aggregates'. 
            # Let's assume standard 'aggregates' method might miss it unless we check 'analytics' path.
            # My list_aggregates looks in .../aggregates/. 
            # Gold path was: "gold/{season}/analytics/".
            
            # Let's customize lookup for Gold or align paths.
            # For robustness, let's look in analytics/
            prefix_path = f"{self.store.prefix}/gold/{season.replace('/', '-')}/analytics/"
            try:
                response = self.store.s3_client.list_objects_v2(Bucket=self.store.bucket_name, Prefix=prefix_path)
                if 'Contents' in response:
                    for obj in response['Contents']:
                        if obj['Key'].endswith('.csv'):
                            df = self.store.read_csv(obj['Key'])
                            if df is not None:
                                gold_dfs.append(df)
            except Exception:
                pass

        if gold_dfs:
            gold_master = pd.concat(gold_dfs, ignore_index=True)
            if "match_id" in gold_master.columns:
                gold_master = gold_master.drop_duplicates(subset=["match_id"], keep="last")
                
            if "season" in gold_master.columns and "matchweek" in gold_master.columns:
                gold_master = gold_master.sort_values(by=["season", "matchweek"])
            
            timestamp = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
            gold_key = f"{self.store.prefix}/gold/master/MU_MASTER_ALL_SEASONS_{timestamp}.csv"
            self.store.upload_csv(gold_master.to_dict('records'), layer="gold", s3_key=gold_key)
            
            logger.info(f"GOLD MASTER UPDATED: {len(gold_master)} records across {len(seasons)} seasons")


    def _transform_flat_data_for_team(self, row: Dict[str, Any], team_aliases: List[str]) -> Dict[str, Any]:
        """Extract statistics specifically for a target team from a flat record."""
        home_team = row.get("home_team")
        away_team = row.get("away_team")
        
        # Determine side
        side = None
        if home_team in team_aliases:
            side = "home"
            team_name = home_team
            opponent = away_team
            team_score = row.get("home_score")
            opp_score = row.get("away_score")
        elif away_team in team_aliases:
            side = "away"
            team_name = away_team
            opponent = home_team
            team_score = row.get("away_score")
            opp_score = row.get("home_score")
        else:
            return None # Skip
            
        # Determine Result
        result = "D"
        try:
            ts = int(team_score)
            os = int(opp_score)
            if ts > os: result = "W"
            elif ts < os: result = "L"
        except:
            pass
        
        # Base Record
        record = {
            "match_id": row.get("match_id"),
            "season": row.get("season"),
            "matchweek": row.get("matchweek"),
            "date": row.get("date"),
            "venue": row.get("venue"),
            "is_home": (side == "home"),
            "team": team_name,
            "opponent": opponent,
            "result": result,
            "goals_scored": team_score,
            "goals_conceded": opp_score,
        }
        
        # Extract stats dynamically based on prefix "home_" or "away_"
        # We look for keys in 'row' that start with the side prefix
        prefix = f"{side}_"
        opponent_prefix = "away_" if side == "home" else "home_"
        
        for k, v in row.items():
            if k.startswith(prefix):
                # e.g. "home_possession" -> "possession"
                clean_key = k[len(prefix):]
                # Avoid collision with base keys if any
                if clean_key not in record:
                    record[clean_key] = v
            # Optional: Include opponent stats? explicit request was "stats belonging to MU"
            # But usually you want 'shots_conceded' etc. 
            # Let's add columns like 'opponent_shots' logic if needed. 
            # For now, stick to "stats belonging to MU" implies their performance.

        return record

    def _flatten_match_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Convert nested match data into a flat dictionary (Silver Layer)."""
        info = data.get("match_info", {})
        stats = data.get("statistics", {})
        
        flat = {
            "match_id": data.get("match_id"),
            "season": data.get("season"),
            "matchweek": data.get("matchweek"),
            "date": info.get("date_time") or info.get("date"),
            "venue": info.get("venue"),
            "referee": info.get("referee"),
            "home_team": info.get("home_team"),
            "away_team": info.get("away_team"),
            "home_score": info.get("home_score"),
            "away_score": info.get("away_score"),
        }
        
        # Flatten Statistics
        for stat_name, val_dict in stats.items():
            if not isinstance(val_dict, dict): 
                continue
                
            clean_name = stat_name.lower().replace(" ", "_").replace("(", "").replace(")", "").replace("%", "").strip("_")
            
            # Handle home values
            h_val = val_dict.get("home")
            if isinstance(h_val, dict) and "value" in h_val:
                flat[f"home_{clean_name}"] = h_val["value"]
                if "percent" in h_val:
                    flat[f"home_{clean_name}_pct"] = h_val["percent"]
            else:
                flat[f"home_{clean_name}"] = h_val

            # Handle away values
            a_val = val_dict.get("away")
            if isinstance(a_val, dict) and "value" in a_val:
                flat[f"away_{clean_name}"] = a_val["value"]
                if "percent" in a_val:
                    flat[f"away_{clean_name}_pct"] = a_val["percent"]
            else:
                flat[f"away_{clean_name}"] = a_val
                
        return flat

    def _clean_match_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Clean raw match data."""
        clean = data.copy()
        
        # 1. Match Info Cleaning
        if "match_info" in clean:
            for k, v in clean["match_info"].items():
                if isinstance(v, str):
                    clean["match_info"][k] = v.strip()
            
            # Ensure scores are integers
            for score_key in ["home_score", "away_score"]:
                val = clean["match_info"].get(score_key)
                if val is not None and not isinstance(val, int):
                    try:
                        clean["match_info"][score_key] = int(val)
                    except (ValueError, TypeError):
                        clean["match_info"][score_key] = 0

        # 2. Statistics Cleaning
        # The 'detailed_statistics' often contains the good stuff in 'top_stats' or other categories
        # But sometimes they are messy. Flatten and clean them.
        cleaned_stats = {}
        
        # Merge 'top_stats' and 'statistics' if they exist, giving priority to 'detailed_statistics'
        raw_stats = {}
        if "statistics" in clean and isinstance(clean["statistics"], dict):
            raw_stats.update(clean["statistics"])
        
        if "detailed_statistics" in clean:
            for cat, content in clean["detailed_statistics"].items():
                if isinstance(content, dict):
                    raw_stats.update(content)

        for stat_name, values in raw_stats.items():
            if not isinstance(values, dict):
                continue
                
            clean_name = stat_name.strip()
            home_val = values.get("home")
            away_val = values.get("away")

            # FIX: Broken Possession Key (e.g. "39.9%60.1%")
            # If the key looks like two percentages concatenated
            if "%" in clean_name and any(c.isdigit() for c in clean_name):
                 # Try to recover possession
                 # Case: "39.9%60.1%" -> Away: 39.9%, Home: Possession (garbage)
                 # We can try to parse the key or look at the values
                 if values.get("home") == "Possession" or values.get("away") == "Possession":
                     clean_name = "Possession"
                     # Try to parse from the broken key if val is bad
                     import re
                     # Find all percentage numbers
                     matches = re.findall(r"(\d+(?:\.\d+)?%)", stat_name)
                     if len(matches) == 2:
                         # Usually order is Home then Away in DOM, but scraper put it in key.
                         # Based on user input: key="39.9%60.1%", away="39.9%".
                         # So the first part of key matches away? Or home? 
                         # Let's trust the one valid value if we have it.
                         if values.get("away") and "%" in str(values.get("away")):
                             away_val = values.get("away")
                             # Calculate home from 100% or use the other match?
                             # Let's just try to parse the non-broken one from matches
                             try:
                                 away_float = float(away_val.strip('%'))
                                 home_val = f"{100 - away_float:.1f}%"
                             except:
                                 pass
            
            # Standardize Value Format
            cleaned_stats[clean_name] = {
                "home": self._parse_stat_value(home_val),
                "away": self._parse_stat_value(away_val)
            }

        clean["statistics"] = cleaned_stats

        # 3. Lineups Cleaning
        if "lineups" in clean:
            for side in ["home", "away"]:
                if side in clean["lineups"]:
                    clean["lineups"][side]["starting_xi"] = self._clean_player_list(
                        clean["lineups"][side].get("starting_xi", []), is_sub=False
                    )
                    clean["lineups"][side]["substitutes"] = self._clean_player_list(
                        clean["lineups"][side].get("substitutes", []), is_sub=True
                    )
        
        return clean

    def _parse_stat_value(self, value):
        """Parse complex stat strings like '43 (49%)' or '108km'."""
        if value is None:
            return None
            
        val_str = str(value).strip()
        
        # Handle "123 (45%)" format
        if "(" in val_str and ")" in val_str:
            import re
            match = re.match(r"([\d\.]+)\s*\(([\d\.]+)%\)", val_str)
            if match:
                return {
                    "value": float(match.group(1)),
                    "percent": float(match.group(2))
                }
        
        # Handle "100.5km"
        if val_str.lower().endswith("km"):
            try:
                return float(val_str.lower().replace("km", ""))
            except ValueError:
                pass
                
        # Handle regular percentages "45%"
        if val_str.endswith("%"):
            try:
                return float(val_str.rstrip("%"))
            except ValueError:
                pass

        # Try Integer
        if val_str.isdigit():
            return int(val_str)
            
        # Try Float
        try:
            return float(val_str)
        except ValueError:
            return val_str

    def _clean_player_list(self, players: List[Dict], is_sub: bool = False) -> List[Dict]:
        """Clean player names and metadata."""
        cleaned = []
        for p in players:
            new_p = p.copy()
            raw_name = new_p.get("name", "")
            
            if is_sub:
                # Format: "Name\nNumber\nValidPosition"
                parts = raw_name.split('\n')
                if len(parts) >= 1:
                    new_p["name"] = parts[0].strip()
                if len(parts) >= 2 and parts[1].isdigit():
                    new_p["number"] = parts[1].strip()
                if len(parts) >= 3:
                     new_p["position"] = parts[2].strip()
            else:
                # Format: "NumberName" e.g. "40Bizot"
                number = new_p.get("number", "")
                if number and raw_name.startswith(number):
                    # Remove the number prefix
                    new_p["name"] = raw_name[len(number):].strip()
            
            cleaned.append(new_p)
        return cleaned

    def _transform_match_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Select specific fields and structure for Gold layer."""
        info = data.get("match_info", {})
        stats = data.get("statistics", {})
        
        # Helper to safely get home/away values from our standardized structure
        def get_stat(name, side, sub_key=None):
            st = stats.get(name, {})
            val = st.get(side)
            if isinstance(val, dict) and sub_key:
                return val.get(sub_key)
            return val

        # Flattened structure for analytics
        record = {
            # Meta
            "match_id": data.get("match_id"),
            "season": data.get("season"),
            "matchweek": data.get("matchweek"),
            "date": info.get("date_time") or info.get("date"), # Handle variations
            "venue": info.get("venue"),
            "referee": info.get("referee"),
            
            # Scores & Teams
            "home_team": info.get("home_team"),
            "away_team": info.get("away_team"),
            "home_score": info.get("home_score"),
            "away_score": info.get("away_score"),
            
            # Key Metrics (xG, Possession, etc.)
            "home_xg": get_stat("XG", "home"),
            "away_xg": get_stat("XG", "away"),
            "home_possession": get_stat("Possession", "home"),
            "away_possession": get_stat("Possession", "away"),
            
            # Attack
            "home_shots": get_stat("Total Shots", "home"),
            "away_shots": get_stat("Total Shots", "away"),
            "home_shots_on_target": get_stat("Shots On Target", "home"),
            "away_shots_on_target": get_stat("Shots On Target", "away"),
            "home_big_chances": get_stat("Big Chances Created", "home"),
            "away_big_chances": get_stat("Big Chances Created", "away"),
            
            # Passing 
            # Note: Handling complex stats (Value vs Percent)
            "home_passes": get_stat("Total Passes", "home", "value"),
            "home_passes_acc": get_stat("Total Passes", "home", "percent"),
            "away_passes": get_stat("Total Passes", "away", "value"),
            "away_passes_acc": get_stat("Total Passes", "away", "percent"),
            
            # Defensive
            "home_tackles_won": get_stat("Tackles Won", "home", "value"),
            "home_tackles_acc": get_stat("Tackles Won", "home", "percent"),
            "away_tackles_won": get_stat("Tackles Won", "away", "value"),
            "away_tackles_acc": get_stat("Tackles Won", "away", "percent"),
            "home_clearances": get_stat("Clearances", "home"),
            "away_clearances": get_stat("Clearances", "away"),
            "home_interceptions": get_stat("Interceptions", "home"),
            "away_interceptions": get_stat("Interceptions", "away"),

            # Discipline
            "home_fouls": get_stat("Fouls", "home"),
            "away_fouls": get_stat("Fouls", "away"),
            "home_yellow_cards": get_stat("Yellow Cards", "home"),
            "away_yellow_cards": get_stat("Yellow Cards", "away"),
            "home_red_cards": get_stat("Red Cards", "home"),
            "away_red_cards": get_stat("Red Cards", "away"),
            
            # Physical
            "home_distance_km": get_stat("Distance Covered", "home"),
            "away_distance_km": get_stat("Distance Covered", "away"),
            "home_sprints": get_stat("Sprints", "home"), # Usually available
            "away_sprints": get_stat("Sprints", "away"),
        }
        
        return record
