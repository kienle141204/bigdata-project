
"""ETL Pipeline for processing Premier League data through Bronze, Silver, and Gold layers."""
import os
import json
import shutil
import tempfile
import pandas as pd
from typing import Dict, Any, List
from loguru import logger
from data.processor import S3DataStore

try:
    from pyspark.sql import SparkSession
    from pyspark.sql import Row
    HAS_SPARK = True
except ImportError:
    logger.warning("PySpark not found. Some ETL features may be disabled.")
    HAS_SPARK = False


class ETLPipeline:
    def __init__(self, bucket_name: str = None, prefix: str = None):
        self.store = S3DataStore(bucket_name=bucket_name, prefix=prefix)
        self.spark = None

    def _init_spark(self):
        """Initialize Spark Session lazily."""
        if self.spark is None and HAS_SPARK:
            try:
                self.spark = SparkSession.builder \
                    .appName("PremierLeagueETL") \
                    .master("local[*]") \
                    .config("spark.driver.host", "127.0.0.1") \
                    .getOrCreate()
                self.spark.sparkContext.setLogLevel("ERROR")
            except Exception as e:
                logger.error(f"Failed to init Spark: {e}")
                self.spark = None

    def process_bronze_to_silver(self, season: str, matchweek: int = None):
        """Clean raw data from Bronze and save to Silver using PySpark."""
        if not HAS_SPARK:
            logger.error("PySpark is required but not installed/working for Bronze -> Silver transformation.")
            return

        self._init_spark()
        if not self.spark:
            logger.error("Spark session could not be initialized.")
            return

        logger.info(f"Starting Bronze -> Silver ETL (PySpark) for Season {season}, MW {matchweek}")
        
        # 1. List files
        files = self.store.list_files(layer="bronze", season=season, matchweek=matchweek, ext="json")
        logger.info(f"Found {len(files)} files in Bronze layer")
        
        if not files:
            return

        # 2. Download to Temp (Simulating S3 access for Spark local)
        with tempfile.TemporaryDirectory() as temp_dir:
            local_input_dir = os.path.join(temp_dir, "input")
            local_output_dir = os.path.join(temp_dir, "output")
            os.makedirs(local_input_dir, exist_ok=True)
            
            logger.info("Downloading files to local temp storage for Spark processing...")
            downloaded_paths = []
            for file_key in files:
                # We need to read content and save locally
                data = self.store.read_json(file_key)
                if data:
                    fname = os.path.basename(file_key)
                    local_path = os.path.join(local_input_dir, fname)
                    with open(local_path, "w", encoding="utf-8") as f:
                        json.dump(data, f, ensure_ascii=False)
                    downloaded_paths.append(local_path)
            
            logger.info(f"Downloaded {len(downloaded_paths)} files. Starting Spark job...")

            # 3. Process with Spark RDD
            sc = self.spark.sparkContext
            rdd = sc.wholeTextFiles(os.path.join(local_input_dir, "*.json"))
            
            # Map Transformations
            processed_rdd = rdd.map(lambda x: json.loads(x[1])) \
                               .map(ETLPipeline._clean_match_data) \
                               .map(ETLPipeline._flatten_match_data)
            
            # Convert to DataFrame
            try:
                # Infer schema from RDD - might be slow for huge datasets but fine here
                # We need to ensure dictionaries are consistent. 
                # Spark might reject if schemas mismatch.
                # Just in case, we can filter None
                processed_rdd = processed_rdd.filter(lambda x: x is not None)
                df = processed_rdd.toDF() # PySpark infers schema automatically
            except Exception as e:
                logger.warning(f"RDD to DataFrame failed: {e}. Trying to sample.")
                return

            # 4. Save to Silver (CSV)
            # Flattening complete via _flatten_match_data, so df should be flat.
            
            df.coalesce(1).write \
                .mode("overwrite") \
                .option("header", "true") \
                .csv(local_output_dir)
            
            logger.info("Spark processing complete. Uploading results...")
            
            # 5. Upload Result
            # Find the part- file
            found = False
            for f in os.listdir(local_output_dir):
                if f.startswith("part-") and f.endswith(".csv"):
                    csv_path = os.path.join(local_output_dir, f)
                    
                    # Read back to upload
                    try:
                        res_df = pd.read_csv(csv_path)
                        
                        # Save Aggregate
                        timestamp = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
                        agg_key = f"{self.store.prefix}/silver/{season.replace('/', '-')}/aggregates/mw{matchweek:02d}_spark_{timestamp}.csv"
                        
                        self.store.upload_csv(res_df.to_dict('records'), layer="silver", s3_key=agg_key)
                        logger.info(f"Uploaded Silver Aggregate: {agg_key}")
                        found = True
                    except Exception as e:
                        logger.error(f"Error uploading CSV: {e}")
                    break
            
            if not found:
                logger.warning("No CSV output found from Spark job.")

        logger.info("Bronze -> Silver ETL complete.")

    def process_silver_to_gold(self, season: str, matchweek: int = None):
        """Read Silver CSVs, filter for Man Utd, and save to Gold as CSV."""
        logger.info(f"Starting Silver -> Gold ETL for Season {season}, MW {matchweek} (Target: Man Utd)")
        
        # Look for aggregates since that's what we produce now
        files = self.store.list_aggregates(layer="silver", season=season)
        logger.info(f"Found {len(files)} aggregate files in Silver layer")
        
        gold_dataset = []
        target_aliases = ["Man Utd", "Manchester United"]
        
        for file_key in files:
            df = self.store.read_csv(file_key)
            if df is None or df.empty:
                continue
            
            for _, row in df.iterrows():
                row_dict = row.to_dict()
                gold_record = self._transform_flat_data_for_team(row_dict, target_aliases)
                if gold_record:
                    gold_dataset.append(gold_record)

        if gold_dataset:
            timestamp = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
            agg_key = f"{self.store.prefix}/gold/{season.replace('/', '-')}/analytics/mu_stats_mw{matchweek}_{timestamp}.csv"
            self.store.upload_csv(gold_dataset, layer="gold", s3_key=agg_key)
            
        logger.info(f"Silver -> Gold complete. Processed {len(gold_dataset)} records for Man Utd.")

    def create_global_aggregates(self, seasons: List[str]):
        """Consolidate ALL data from processed seasons into Master CSVs."""
        logger.info("="*30)
        logger.info("CREATING GLOBAL MASTER DATASETS")
        logger.info("="*30)
        
        silver_dfs = []
        for season in seasons:
            agg_files = self.store.list_aggregates(layer="silver", season=season)
            for f in agg_files:
                df = self.store.read_csv(f)
                if df is not None and not df.empty:
                    silver_dfs.append(df)
        
        if silver_dfs:
            silver_master = pd.concat(silver_dfs, ignore_index=True)
            if "match_id" in silver_master.columns:
                silver_master = silver_master.drop_duplicates(subset=["match_id"], keep="last")
            if "season" in silver_master.columns and "matchweek" in silver_master.columns:
                silver_master = silver_master.sort_values(by=["season", "matchweek"])

            timestamp = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
            silver_key = f"{self.store.prefix}/silver/master/MASTER_ALL_SEASONS_{timestamp}.csv"
            self.store.upload_csv(silver_master.to_dict('records'), layer="silver", s3_key=silver_key)
            logger.info(f"SILVER MASTER UPDATED")

        # Gold
        gold_dfs = []
        for season in seasons:
             prefix_path = f"{self.store.prefix}/gold/{season.replace('/', '-')}/analytics/"
             try:
                response = self.store.s3_client.list_objects_v2(Bucket=self.store.bucket_name, Prefix=prefix_path)
                if 'Contents' in response:
                    for obj in response['Contents']:
                        if obj['Key'].endswith('.csv'):
                            df = self.store.read_csv(obj['Key'])
                            if df is not None: gold_dfs.append(df)
             except: pass

        if gold_dfs:
            gold_master = pd.concat(gold_dfs, ignore_index=True)
            if "match_id" in gold_master.columns:
                gold_master = gold_master.drop_duplicates(subset=["match_id"], keep="last")
            
            timestamp = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
            gold_key = f"{self.store.prefix}/gold/master/MU_MASTER_ALL_SEASONS_{timestamp}.csv"
            self.store.upload_csv(gold_master.to_dict('records'), layer="gold", s3_key=gold_key)
            logger.info(f"GOLD MASTER UPDATED")

    # -------------------------------------------------------------------------
    # STATIC HELPER METHODS
    # -------------------------------------------------------------------------

    @staticmethod
    def _transform_flat_data_for_team(row: Dict[str, Any], team_aliases: List[str]) -> Dict[str, Any]:
        """Extract statistics specifically for a target team from a flat record."""
        # Clean float keys if any
        cl_row = {k: v for k, v in row.items()}
        
        home_team = cl_row.get("home_team")
        away_team = cl_row.get("away_team")
        
        side = None
        if home_team in team_aliases:
            side = "home"
            team_name = home_team
            opponent = away_team
            team_score = cl_row.get("home_score")
            opp_score = cl_row.get("away_score")
        elif away_team in team_aliases:
            side = "away"
            team_name = away_team
            opponent = home_team
            team_score = cl_row.get("away_score")
            opp_score = cl_row.get("home_score")
        else:
            return None 
            
        result = "D"
        try:
            ts = float(team_score) if team_score is not None else 0
            os = float(opp_score) if opp_score is not None else 0
            if ts > os: result = "W"
            elif ts < os: result = "L"
        except: pass
        
        record = {
            "match_id": cl_row.get("match_id"),
            "season": cl_row.get("season"),
            "matchweek": cl_row.get("matchweek"),
            "date": cl_row.get("date"),
            "venue": cl_row.get("venue"),
            "is_home": (side == "home"),
            "team": team_name,
            "opponent": opponent,
            "result": result,
            "goals_scored": team_score,
            "goals_conceded": opp_score,
        }
        
        prefix = f"{side}_"
        for k, v in cl_row.items():
            if k.startswith(prefix):
                clean_key = k[len(prefix):]
                if clean_key not in record:
                    record[clean_key] = v
        return record

    @staticmethod
    def _flatten_match_data(data: Dict[str, Any]) -> Dict[str, Any]:
        """Convert nested match data into a flat dictionary."""
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
        
        for stat_name, val_dict in stats.items():
            if not isinstance(val_dict, dict): continue
                
            clean_name = stat_name.lower().replace(" ", "_").replace("(", "").replace(")", "").replace("%", "").strip("_")
            
            # Home
            h_val = val_dict.get("home")
            if isinstance(h_val, dict) and "value" in h_val:
                flat[f"home_{clean_name}"] = h_val["value"]
                if "percent" in h_val: flat[f"home_{clean_name}_pct"] = h_val["percent"]
            else:
                flat[f"home_{clean_name}"] = h_val

            # Away
            a_val = val_dict.get("away")
            if isinstance(a_val, dict) and "value" in a_val:
                flat[f"away_{clean_name}"] = a_val["value"]
                if "percent" in a_val: flat[f"away_{clean_name}_pct"] = a_val["percent"]
            else:
                flat[f"away_{clean_name}"] = a_val
                
        return flat

    @staticmethod
    def _clean_match_data(data: Dict[str, Any]) -> Dict[str, Any]:
        """Clean raw match data."""
        clean = data.copy()
        
        if "match_info" in clean:
            for k, v in clean["match_info"].items():
                if isinstance(v, str): clean["match_info"][k] = v.strip()
            for score_key in ["home_score", "away_score"]:
                val = clean["match_info"].get(score_key)
                if val is not None and not isinstance(val, int):
                    try: clean["match_info"][score_key] = int(val)
                    except: clean["match_info"][score_key] = 0

        raw_stats = {}
        if "statistics" in clean and isinstance(clean["statistics"], dict):
            raw_stats.update(clean["statistics"])
        if "detailed_statistics" in clean and isinstance(clean["detailed_statistics"], dict):
             for cat, content in clean["detailed_statistics"].items():
                if isinstance(content, dict): raw_stats.update(content)

        cleaned_stats = {}
        for stat_name, values in raw_stats.items():
            if not isinstance(values, dict): continue
            
            clean_name = stat_name.strip()
            home_val = values.get("home")
            away_val = values.get("away")

            if "%" in clean_name and any(c.isdigit() for c in clean_name):
                 if values.get("home") == "Possession" or values.get("away") == "Possession":
                     clean_name = "Possession"
            
            cleaned_stats[clean_name] = {
                "home": ETLPipeline._parse_stat_value(home_val),
                "away": ETLPipeline._parse_stat_value(away_val)
            }
        clean["statistics"] = cleaned_stats
        
        return clean

    @staticmethod
    def _parse_stat_value(value):
        """Parse complex stat strings."""
        if value is None: return None
        val_str = str(value).strip()
        
        if "(" in val_str and ")" in val_str:
            import re
            match = re.search(r"([\d\.]+)\s*\(([\d\.]+)%\)", val_str)
            if match:
                return {"value": float(match.group(1)), "percent": float(match.group(2))}
        
        if val_str.lower().endswith("km"):
            try: return float(val_str.lower().replace("km", ""))
            except: pass
            
        if val_str.endswith("%"):
            try: return float(val_str.rstrip("%"))
            except: pass
            
        if val_str.isdigit(): return int(val_str)
        try: return float(val_str)
        except: return val_str
