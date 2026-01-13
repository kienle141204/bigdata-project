import os
import sys
from typing import List, Dict
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, when, lit, to_timestamp, trim, upper, broadcast, sum, row_number, regexp_replace
)
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType, DecimalType, StringType, BooleanType, TimestampType, StructType, StructField, DoubleType, LongType
from dotenv import load_dotenv

project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

from Spark.config.spark_config import get_s3_path
from Spark.connectors.spark_mysql_connector import create_spark_session_with_mysql, write_mysql_table

load_dotenv()


class S3ToMySQLTransformer:
    def __init__(self, streaming_mode: bool = False):
        self.spark = None
        self.jdbc_url = None
        self.conn_props = None
        self.streaming_mode = streaming_mode
        self.streaming_query = None

    def initialize(self):
        app_name = "S3ToMySQLStreaming" if self.streaming_mode else "S3ToMySQLTransformer"

        self.spark, self.jdbc_url, self.conn_props = create_spark_session_with_mysql(
            app_name=app_name,
            include_s3=True
        )

        if self.streaming_mode:
            self.spark.conf.set("spark.sql.streaming.stopGracefullyOnShutdown", "true")
            self.spark.conf.set("spark.sql.adaptive.enabled", "true")
            self.spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")

    def read_csv_from_s3(self, s3_path: str) -> DataFrame:
        df = self.spark.read.csv(
            s3_path,
            header=True,
            inferSchema=True,
            nullValue="",
            emptyValue=None,
            nanValue=None
        )
        return df

    def read_gold_files(self, season: str = None, file_type: str = None):
        bucket = os.getenv("AWS_S3_BUCKET", "")
        prefix = os.getenv("AWS_S3_PREFIX", "premier_league")

        files = {}

        if file_type == "matches" or file_type is None:
            if season:
                s3_path = get_s3_path(bucket, f"{prefix}/gold/{season}/**/matches.csv", "")
            else:
                s3_path = get_s3_path(bucket, f"{prefix}/gold/**/matches.csv", "")
            try:
                files["matches"] = self.read_csv_from_s3(s3_path)
            except Exception as e:
                files["matches"] = None

        if file_type == "players" or file_type is None:
            if season:
                s3_path = get_s3_path(bucket, f"{prefix}/gold/{season}/**/players.csv", "")
            else:
                s3_path = get_s3_path(bucket, f"{prefix}/gold/**/players.csv", "")
            try:
                files["players"] = self.read_csv_from_s3(s3_path)
            except Exception as e:
                files["players"] = None

        if file_type == "events" or file_type is None:
            if season:
                s3_path = get_s3_path(bucket, f"{prefix}/gold/{season}/**/events.csv", "")
            else:
                s3_path = get_s3_path(bucket, f"{prefix}/gold/**/events.csv", "")
            try:
                files["events"] = self.read_csv_from_s3(s3_path)
            except Exception as e:
                files["events"] = None

        if file_type == "analytics" or file_type is None:
            if season:
                s3_path = get_s3_path(bucket, f"{prefix}/gold/{season}/analytics/*.csv", "")
            else:
                s3_path = get_s3_path(bucket, f"{prefix}/gold/**/analytics/*.csv", "")
            try:
                files["analytics"] = self.read_csv_from_s3(s3_path)
            except Exception as e:
                files["analytics"] = None

        if file_type == "master" or file_type is None:
            s3_path = get_s3_path(bucket, f"{prefix}/gold/master/*.csv", "")
            try:
                files["master"] = self.read_csv_from_s3(s3_path)
            except Exception as e:
                files["master"] = None

        return files

    def _table_exists(self, table_name: str) -> bool:
        try:
            self.spark.read.jdbc(
                url=self.jdbc_url,
                table=table_name,
                properties=self.conn_props
            ).limit(1).collect()
            return True
        except:
            return False

    def _get_or_create_season_ids(self, matches_df: DataFrame) -> Dict[str, int]:
        if not self._table_exists("mua_giai"):
            raise ValueError("Bảng 'mua_giai' chưa tồn tại!")

        seasons_df = matches_df.select("season").distinct().filter(col("season").isNotNull())
        seasons_list = [row.season for row in seasons_df.collect() if row.season]

        if not seasons_list:
            return {}

        try:
            existing_seasons_df = self.spark.read.jdbc(
                url=self.jdbc_url,
                table="mua_giai",
                properties=self.conn_props
            )
            existing_seasons = {row.ten_mua_giai: row.id for row in existing_seasons_df.collect()}
        except:
            existing_seasons = {}

        new_seasons = [s for s in seasons_list if s and s not in existing_seasons]
        if new_seasons:
            new_seasons_data = []
            for season in new_seasons:
                year_start = None
                try:
                    if '-' in season:
                        parts = season.split('-')
                        if len(parts) == 2:
                            year_start = int(parts[0])
                    else:
                        if len(season) >= 4:
                            year_start = int(season[:4])
                except (ValueError, IndexError):
                    year_start = None

                new_seasons_data.append((season, year_start))

            schema = StructType([
                StructField("ten_mua_giai", StringType(), False),
                StructField("nam_bat_dau", IntegerType(), True)
            ])

            new_seasons_df = self.spark.createDataFrame(
                new_seasons_data,
                schema=schema
            )

            try:
                write_mysql_table(
                    df=new_seasons_df,
                    jdbc_url=self.jdbc_url,
                    table_name="mua_giai",
                    connection_properties=self.conn_props,
                    mode="append"
                )
            except Exception:
                pass

            try:
                all_seasons_df = self.spark.read.jdbc(
                    url=self.jdbc_url,
                    table="mua_giai",
                    properties=self.conn_props
                )
                existing_seasons = {row.ten_mua_giai: row.id for row in all_seasons_df.collect()}
            except:
                pass

        return existing_seasons

    def _get_or_create_team_ids(self, teams_list: List[str]) -> Dict[str, int]:
        if not self._table_exists("doi_bong"):
            raise ValueError("Bảng 'doi_bong' chưa tồn tại!")

        if not teams_list:
            return {}

        try:
            existing_teams_df = self.spark.read.jdbc(
                url=self.jdbc_url,
                table="doi_bong",
                properties=self.conn_props
            )
            existing_teams = {row.ten_doi: row.id for row in existing_teams_df.collect()}
        except:
            existing_teams = {}

        new_teams = [t for t in teams_list if t and t not in existing_teams]
        if new_teams:
            new_teams_df = self.spark.createDataFrame(
                [(team,) for team in new_teams],
                ["ten_doi"]
            )

            try:
                write_mysql_table(
                    df=new_teams_df,
                    jdbc_url=self.jdbc_url,
                    table_name="doi_bong",
                    connection_properties=self.conn_props,
                    mode="append"
                )
            except Exception:
                pass

            try:
                all_teams_df = self.spark.read.jdbc(
                    url=self.jdbc_url,
                    table="doi_bong",
                    properties=self.conn_props
                )
                existing_teams = {row.ten_doi: row.id for row in all_teams_df.collect()}
            except:
                pass

        return existing_teams

    def _get_or_create_round_ids(self, season_ids: Dict[str, int], rounds_data: List[tuple]) -> Dict[tuple, int]:
        if not self._table_exists("vong_dau"):
            raise ValueError("Bảng 'vong_dau' chưa tồn tại!")

        if not rounds_data:
            return {}

        try:
            existing_rounds_df = self.spark.read.jdbc(
                url=self.jdbc_url,
                table="vong_dau",
                properties=self.conn_props
            )
            existing_rounds = {
                (row.id_mua_giai, row.so_vong_dau): row.id
                for row in existing_rounds_df.collect()
            }
        except:
            existing_rounds = {}

        new_rounds_data = []
        for season_name, round_num in rounds_data:
            if season_name in season_ids:
                season_id = season_ids[season_name]
                key = (season_id, round_num)
                if key not in existing_rounds:
                    new_rounds_data.append((season_id, round_num))

        if new_rounds_data:
            new_rounds_df = self.spark.createDataFrame(
                new_rounds_data,
                ["id_mua_giai", "so_vong_dau"]
            )

            try:
                write_mysql_table(
                    df=new_rounds_df,
                    jdbc_url=self.jdbc_url,
                    table_name="vong_dau",
                    connection_properties=self.conn_props,
                    mode="append"
                )
            except Exception:
                pass

            try:
                all_rounds_df = self.spark.read.jdbc(
                    url=self.jdbc_url,
                    table="vong_dau",
                    properties=self.conn_props
                )
                existing_rounds = {
                    (row.id_mua_giai, row.so_vong_dau): row.id
                    for row in all_rounds_df.collect()
                }
            except:
                pass

        return existing_rounds

    def _is_empty(self, df: DataFrame) -> bool:
        if df is None:
            return True
        try:
            return df.rdd.isEmpty()
        except:
            return True

    def load_matches_data(self, matches_df: DataFrame):
        if matches_df is None or self._is_empty(matches_df):
            return
        
        season_ids = self._get_or_create_season_ids(matches_df)
        if not season_ids:
            return

        home_teams = matches_df.select("home_team").distinct().filter(col("home_team").isNotNull())
        away_teams = matches_df.select("away_team").distinct().filter(col("away_team").isNotNull())
        all_teams = home_teams.union(away_teams).distinct()
        teams_list = [row.home_team for row in all_teams.collect() if row.home_team]
        team_ids = self._get_or_create_team_ids(teams_list)

        self._load_season_teams(matches_df, season_ids, team_ids)

        rounds_data = matches_df.select("season", "matchweek").distinct().filter(
            col("season").isNotNull() & col("matchweek").isNotNull()
        )
        rounds_list = [(row.season, int(row.matchweek)) for row in rounds_data.collect()]
        round_ids = self._get_or_create_round_ids(season_ids, rounds_list)

        self._load_matches(matches_df, season_ids, team_ids, round_ids)
        self._update_round_match_count()
        self._load_match_results(matches_df, team_ids)
        self._update_season_team_stats()
        self._load_match_stats(matches_df, team_ids)
        self._load_lineups(matches_df, team_ids)
        self._load_goals(matches_df, team_ids)

    def _load_season_teams(self, matches_df: DataFrame, season_ids: Dict[str, int], team_ids: Dict[str, int]):
        if not self._table_exists("mua_giai_doi_bong"):
            return

        if not season_ids or not team_ids:
            return

        season_teams_data = []
        for season_name, season_id in season_ids.items():
            for team_name, team_id in team_ids.items():
                season_teams_data.append((season_id, team_id))

        if not season_teams_data:
            return

        schema = StructType([
            StructField("id_mua_giai", LongType(), False),
            StructField("id_doi_bong", LongType(), False)
        ])

        season_teams_df = self.spark.createDataFrame(season_teams_data, schema=schema)

        try:
            existing_df = self.spark.read.jdbc(
                url=self.jdbc_url,
                table="(SELECT id_mua_giai, id_doi_bong FROM mua_giai_doi_bong) AS existing",
                properties=self.conn_props
            )
            if not self._is_empty(existing_df):
                season_teams_df = season_teams_df.join(
                    broadcast(existing_df),
                    ["id_mua_giai", "id_doi_bong"],
                    "left_anti"
                )
        except Exception:
            pass

        if not self._is_empty(season_teams_df):
            write_mysql_table(
                df=season_teams_df,
                jdbc_url=self.jdbc_url,
                table_name="mua_giai_doi_bong",
                connection_properties=self.conn_props,
                mode="append"
            )

    def _load_matches(self, matches_df: DataFrame, season_ids: Dict[str, int], team_ids: Dict[str, int], round_ids: Dict[tuple, int]):
        if not self._table_exists("tran_dau"):
            return

        seasons_lookup = self.spark.createDataFrame(
            [(name, sid) for name, sid in season_ids.items()],
            ["season", "id_mua_giai"]
        ).cache()
        teams_lookup = self.spark.createDataFrame(
            [(name, tid) for name, tid in team_ids.items()],
            ["team_name", "id"]
        ).cache()

        available_cols = set(matches_df.columns)

        date_col = None
        for possible_date_col in ["date", "ngay_gio", "match_date", "datetime"]:
            if possible_date_col in available_cols:
                date_col = possible_date_col
                break

        select_exprs = [
            col("match_id").alias("id_tran_dau"),
            col("season"),
            col("matchweek").cast(IntegerType()).alias("matchweek"),
        ]

        if date_col:
            select_exprs.append(
                when(col(date_col).isNotNull(), to_timestamp(col(date_col))).otherwise(None).alias("ngay_gio")
            )
        else:
            select_exprs.append(lit(None).cast(TimestampType()).alias("ngay_gio"))

        if "venue" in available_cols:
            select_exprs.append(col("venue").alias("san_van_dong"))
        else:
            select_exprs.append(lit(None).cast(StringType()).alias("san_van_dong"))

        if "referee" in available_cols:
            select_exprs.append(col("referee").alias("trong_tai"))
        else:
            select_exprs.append(lit(None).cast(StringType()).alias("trong_tai"))

        if "url" in available_cols:
            select_exprs.append(col("url"))
        else:
            select_exprs.append(lit(None).cast(StringType()).alias("url"))

        if "scraped_at" in available_cols:
            select_exprs.append(
                when(col("scraped_at").isNotNull(), to_timestamp(col("scraped_at"))).otherwise(None).alias("thoi_gian_scrape")
            )
        else:
            select_exprs.append(lit(None).cast(TimestampType()).alias("thoi_gian_scrape"))

        select_exprs.extend([
            col("home_team").alias("home_team_name"),
            col("away_team").alias("away_team_name")
        ])

        matches_prep = matches_df.select(*select_exprs).filter(
            col("id_tran_dau").isNotNull() &
            col("season").isNotNull() &
            col("matchweek").isNotNull()
        )

        matches_with_season = matches_prep.join(
            broadcast(seasons_lookup), "season", "inner"
        )

        matches_with_home = matches_with_season.join(
            broadcast(teams_lookup.alias("home_lookup")),
            col("home_team_name") == col("home_lookup.team_name"),
            "inner"
        )

        matches_with_ids = matches_with_home.join(
            broadcast(teams_lookup.alias("away_lookup")),
            col("away_team_name") == col("away_lookup.team_name"),
            "inner"
        )

        try:
            rounds_lookup = self.spark.read.jdbc(
                url=self.jdbc_url,
                table="vong_dau",
                properties=self.conn_props
            ).select(
                col("id_mua_giai"),
                col("so_vong_dau").alias("matchweek"),
                col("id").alias("id_vong_dau")
            ).cache()
        except Exception:
            rounds_lookup_data = [
                (sid, rnum, rid)
                for (sid, rnum), rid in round_ids.items()
            ]
            rounds_lookup = self.spark.createDataFrame(
                rounds_lookup_data,
                ["id_mua_giai", "matchweek", "id_vong_dau"]
            ).cache()

        matches_final = matches_with_ids.join(
            broadcast(rounds_lookup),
            (matches_with_ids["id_mua_giai"] == rounds_lookup["id_mua_giai"]) &
            (matches_with_ids["matchweek"] == rounds_lookup["matchweek"]),
            "inner"
        ).select(
            col("id_tran_dau"),
            col("id_vong_dau"),
            col("home_lookup.id").alias("id_doi_nha"),
            col("away_lookup.id").alias("id_doi_khach"),
            col("ngay_gio"),
            col("san_van_dong"),
            col("trong_tai"),
            col("url"),
            col("thoi_gian_scrape")
        ).dropDuplicates(["id_tran_dau"])

        existing_ids = None
        max_retries = 3
        for retry in range(max_retries):
            try:
                existing_ids = self.spark.read.jdbc(
                    url=self.jdbc_url,
                    table="(SELECT DISTINCT id_tran_dau FROM tran_dau) AS existing_ids",
                    properties=self.conn_props
                )
                break
            except Exception:
                if retry < max_retries - 1:
                    import time
                    time.sleep(2)
                else:
                    existing_ids = None

        if existing_ids is not None and not self._is_empty(existing_ids):
            matches_final = matches_final.join(
                broadcast(existing_ids),
                matches_final["id_tran_dau"] == existing_ids["id_tran_dau"],
                "left_anti"
            )

        seasons_lookup.unpersist()
        teams_lookup.unpersist()
        rounds_lookup.unpersist()

        if not self._is_empty(matches_final):
            write_mysql_table(
                df=matches_final,
                jdbc_url=self.jdbc_url,
                table_name="tran_dau",
                connection_properties=self.conn_props,
                mode="append"
            )

    def _load_match_results(self, matches_df: DataFrame, team_ids: Dict[str, int]):
        if not self._table_exists("ket_qua_tran_dau"):
            return

        if not self._table_exists("tran_dau"):
            return

        try:
            tran_dau_df = self.spark.read.jdbc(
                url=self.jdbc_url,
                table="tran_dau",
                properties=self.conn_props
            ).select("id", "id_tran_dau").alias("td")
        except Exception:
            return

        teams_lookup = self.spark.createDataFrame(
            [(name, tid) for name, tid in team_ids.items()],
            ["team_name", "id_doi_bong"]
        )

        available_cols = set(matches_df.columns)

        score_cols = {}
        for col_name in ["home_score", "score_home", "home_goals"]:
            if col_name in available_cols:
                score_cols["home_score"] = col_name
                break

        for col_name in ["away_score", "score_away", "away_goals"]:
            if col_name in available_cols:
                score_cols["away_score"] = col_name
                break

        if "home_score" not in score_cols or "away_score" not in score_cols:
            return

        matches_with_scores = matches_df.select(
            col("match_id").alias("id_tran_dau"),
            col("home_team").alias("home_team_name"),
            col("away_team").alias("away_team_name"),
            col(score_cols["home_score"]).cast(IntegerType()).alias("home_score"),
            col(score_cols["away_score"]).cast(IntegerType()).alias("away_score")
        ).filter(
            col("id_tran_dau").isNotNull() &
            col("home_team_name").isNotNull() &
            col("away_team_name").isNotNull() &
            col("home_score").isNotNull() &
            col("away_score").isNotNull()
        )

        matches_with_tran_dau = matches_with_scores.join(
            broadcast(tran_dau_df),
            matches_with_scores["id_tran_dau"] == col("td.id_tran_dau"),
            "inner"
        )

        matches_with_home_team = matches_with_tran_dau.join(
            broadcast(teams_lookup.alias("home_lookup")),
            matches_with_tran_dau["home_team_name"] == col("home_lookup.team_name"),
            "inner"
        )

        matches_with_teams = matches_with_home_team.join(
            broadcast(teams_lookup.alias("away_lookup")),
            matches_with_home_team["away_team_name"] == col("away_lookup.team_name"),
            "inner"
        )

        def create_result_df(is_home: bool):
            team_col = "home_lookup.id_doi_bong" if is_home else "away_lookup.id_doi_bong"
            opponent_col = "away_lookup.id_doi_bong" if is_home else "home_lookup.id_doi_bong"
            goals_scored_col = "home_score" if is_home else "away_score"
            goals_conceded_col = "away_score" if is_home else "home_score"

            return matches_with_teams.select(
                col("td.id").alias("id_tran_dau"),
                col(team_col).alias("id_doi"),
                col(opponent_col).alias("id_doi_thu"),
                lit(1 if is_home else 0).cast(BooleanType()).alias("la_doi_nha"),
                when(col(goals_scored_col) > col(goals_conceded_col), lit("W"))
                .when(col(goals_scored_col) < col(goals_conceded_col), lit("L"))
                .otherwise(lit("D")).alias("ket_qua"),
                col(goals_scored_col).cast(IntegerType()).alias("ban_thang_ghi_duoc"),
                col(goals_conceded_col).cast(IntegerType()).alias("ban_thua"),
                (when(col(goals_scored_col) > col(goals_conceded_col), lit(3))
                 .when(col(goals_scored_col) < col(goals_conceded_col), lit(0))
                 .otherwise(lit(1))).cast(IntegerType()).alias("tong_diem")
            )

        home_results = create_result_df(True)
        away_results = create_result_df(False)
        all_results = home_results.union(away_results).dropDuplicates(["id_tran_dau", "id_doi"])

        if not self._is_empty(all_results):
            try:
                existing_df = self.spark.read.jdbc(
                    url=self.jdbc_url,
                    table="(SELECT id_tran_dau, id_doi FROM ket_qua_tran_dau) AS existing",
                    properties=self.conn_props
                )
                if not self._is_empty(existing_df):
                    all_results = all_results.join(
                        broadcast(existing_df),
                        (all_results["id_tran_dau"] == existing_df["id_tran_dau"]) &
                        (all_results["id_doi"] == existing_df["id_doi"]),
                        "left_anti"
                    )
            except Exception:
                pass

        if not self._is_empty(all_results):
            write_mysql_table(
                df=all_results,
                jdbc_url=self.jdbc_url,
                table_name="ket_qua_tran_dau",
                connection_properties=self.conn_props,
                mode="append"
            )

    def _update_round_match_count(self):
        if not self._table_exists("vong_dau"):
            return

        try:
            tran_dau_df = self.spark.read.jdbc(
                url=self.jdbc_url,
                table="tran_dau",
                properties=self.conn_props
            )

            match_counts = tran_dau_df.groupBy("id_vong_dau").count().select(
                col("id_vong_dau").alias("id"),
                col("count").alias("so_tran_dau")
            )

            if not self._is_empty(match_counts):
                update_records = match_counts.collect()

                try:
                    import pymysql
                except ImportError:
                    return

                db_host = os.getenv("MYSQL_HOST", "localhost")
                db_port = int(os.getenv("MYSQL_PORT", "3306"))
                db_user = os.getenv("MYSQL_USER", "root")
                db_password = os.getenv("MYSQL_PASSWORD", "")
                db_name = os.getenv("MYSQL_DATABASE", "defaultdb")

                try:
                    conn = pymysql.connect(
                        host=db_host,
                        port=db_port,
                        user=db_user,
                        password=db_password,
                        database=db_name
                    )
                    cursor = conn.cursor()

                    update_sql = """
                        UPDATE vong_dau
                        SET so_tran_dau = %s
                        WHERE id = %s
                    """

                    update_data = [
                        (
                            int(row.so_tran_dau) if row.so_tran_dau else 0,
                            int(row.id)
                        )
                        for row in update_records
                    ]

                    cursor.executemany(update_sql, update_data)
                    conn.commit()
                    cursor.close()
                    conn.close()
                except Exception:
                    pass
        except Exception:
            pass

    def _update_season_team_stats(self):
        if not self._table_exists("mua_giai_doi_bong"):
            return

        try:
            ket_qua_df = self.spark.read.jdbc(
                url=self.jdbc_url,
                table="ket_qua_tran_dau",
                properties=self.conn_props
            )

            tran_dau_df = self.spark.read.jdbc(
                url=self.jdbc_url,
                table="tran_dau",
                properties=self.conn_props
            ).select("id", "id_vong_dau").alias("td")

            vong_dau_df = self.spark.read.jdbc(
                url=self.jdbc_url,
                table="vong_dau",
                properties=self.conn_props
            ).select("id", "id_mua_giai").alias("vd")

            stats_with_season = ket_qua_df.join(
                broadcast(tran_dau_df),
                ket_qua_df["id_tran_dau"] == col("td.id"),
                "inner"
            ).join(
                broadcast(vong_dau_df),
                col("td.id_vong_dau") == col("vd.id"),
                "inner"
            )

            stats_agg = stats_with_season.groupBy(
                col("vd.id_mua_giai"),
                col("id_doi")
            ).agg(
                sum(when(col("ket_qua") == "W", 1).otherwise(0)).alias("so_tran_thang"),
                sum(when(col("ket_qua") == "D", 1).otherwise(0)).alias("so_tran_hoa"),
                sum(when(col("ket_qua") == "L", 1).otherwise(0)).alias("so_tran_thua"),
                sum(when(col("ban_thang_ghi_duoc").isNotNull(), col("ban_thang_ghi_duoc")).otherwise(0)).alias("tong_ban_thang"),
                sum(when(col("ban_thua").isNotNull(), col("ban_thua")).otherwise(0)).alias("tong_ban_thua"),
                sum(when(col("ket_qua") == "W", 3)
                    .when(col("ket_qua") == "D", 1)
                    .otherwise(0)).alias("tong_diem")
            ).select(
                col("vd.id_mua_giai").alias("id_mua_giai"),
                col("id_doi").alias("id_doi_bong"),
                col("so_tran_thang").cast(IntegerType()).alias("so_tran_thang"),
                col("so_tran_hoa").cast(IntegerType()).alias("so_tran_hoa"),
                col("so_tran_thua").cast(IntegerType()).alias("so_tran_thua"),
                col("tong_ban_thang").cast(IntegerType()).alias("tong_ban_thang"),
                col("tong_ban_thua").cast(IntegerType()).alias("tong_ban_thua"),
                (col("tong_ban_thang") - col("tong_ban_thua")).cast(IntegerType()).alias("hieu_so"),
                col("tong_diem").cast(IntegerType()).alias("tong_diem")
            )
            
            if self._is_empty(stats_agg):
                return

            window_spec = Window.partitionBy("id_mua_giai").orderBy(
                col("tong_diem").desc(),
                col("hieu_so").desc(),
                col("tong_ban_thang").desc(),
                col("so_tran_thang").desc()
            )

            try:
                existing_df = self.spark.read.jdbc(
                    url=self.jdbc_url,
                    table="mua_giai_doi_bong",
                    properties=self.conn_props
                )

                if not self._is_empty(existing_df):
                    update_df = existing_df.join(
                        stats_agg,
                        (existing_df["id_mua_giai"] == stats_agg["id_mua_giai"]) &
                        (existing_df["id_doi_bong"] == stats_agg["id_doi_bong"]),
                        "left"
                    ).select(
                        existing_df["id"],
                        existing_df["id_mua_giai"],
                        existing_df["id_doi_bong"],
                        when(stats_agg["so_tran_thang"].isNotNull(), stats_agg["so_tran_thang"]).otherwise(lit(0)).cast(IntegerType()).alias("so_tran_thang"),
                        when(stats_agg["so_tran_hoa"].isNotNull(), stats_agg["so_tran_hoa"]).otherwise(lit(0)).cast(IntegerType()).alias("so_tran_hoa"),
                        when(stats_agg["so_tran_thua"].isNotNull(), stats_agg["so_tran_thua"]).otherwise(lit(0)).cast(IntegerType()).alias("so_tran_thua"),
                        when(stats_agg["tong_ban_thang"].isNotNull(), stats_agg["tong_ban_thang"]).otherwise(lit(0)).cast(IntegerType()).alias("tong_ban_thang"),
                        when(stats_agg["tong_ban_thua"].isNotNull(), stats_agg["tong_ban_thua"]).otherwise(lit(0)).cast(IntegerType()).alias("tong_ban_thua"),
                        when(stats_agg["hieu_so"].isNotNull(), stats_agg["hieu_so"]).otherwise(lit(0)).cast(IntegerType()).alias("hieu_so"),
                        when(stats_agg["tong_diem"].isNotNull(), stats_agg["tong_diem"]).otherwise(lit(0)).cast(IntegerType()).alias("tong_diem")
                    )

                    window_spec = Window.partitionBy("id_mua_giai").orderBy(
                        col("tong_diem").desc(),
                        col("hieu_so").desc(),
                        col("tong_ban_thang").desc(),
                        col("so_tran_thang").desc()
                    )

                    update_df = update_df.withColumn(
                        "vi_tri_bang_xep_hang",
                        row_number().over(window_spec).cast(IntegerType())
                    )

                    update_records = update_df.collect()
                    try:
                        import pymysql
                    except ImportError:
                        return

                    db_host = os.getenv("MYSQL_HOST", "localhost")
                    db_port = int(os.getenv("MYSQL_PORT", "3306"))
                    db_user = os.getenv("MYSQL_USER", "root")
                    db_password = os.getenv("MYSQL_PASSWORD", "")
                    db_name = os.getenv("MYSQL_DATABASE", "defaultdb")

                    try:
                        conn = pymysql.connect(
                            host=db_host,
                            port=db_port,
                            user=db_user,
                            password=db_password,
                            database=db_name
                        )
                        cursor = conn.cursor()

                        update_sql = """
                            UPDATE mua_giai_doi_bong
                            SET so_tran_thang = %s,
                                so_tran_hoa = %s,
                                so_tran_thua = %s,
                                tong_ban_thang = %s,
                                tong_ban_thua = %s,
                                hieu_so = %s,
                                tong_diem = %s,
                                vi_tri_bang_xep_hang = %s
                            WHERE id_mua_giai = %s AND id_doi_bong = %s
                        """

                        update_data = [
                            (
                                int(row.so_tran_thang) if row.so_tran_thang else 0,
                                int(row.so_tran_hoa) if row.so_tran_hoa else 0,
                                int(row.so_tran_thua) if row.so_tran_thua else 0,
                                int(row.tong_ban_thang) if row.tong_ban_thang else 0,
                                int(row.tong_ban_thua) if row.tong_ban_thua else 0,
                                int(row.hieu_so) if row.hieu_so else 0,
                                int(row.tong_diem) if row.tong_diem else 0,
                                int(row.vi_tri_bang_xep_hang) if row.vi_tri_bang_xep_hang else None,
                                int(row.id_mua_giai),
                                int(row.id_doi_bong)
                            )
                            for row in update_records
                        ]

                        cursor.executemany(update_sql, update_data)
                        conn.commit()

                        delete_sql = """
                            DELETE FROM mua_giai_doi_bong
                            WHERE so_tran_thang = 0 
                              AND so_tran_hoa = 0 
                              AND so_tran_thua = 0 
                              AND tong_ban_thang = 0 
                              AND tong_ban_thua = 0 
                              AND hieu_so = 0 
                              AND tong_diem = 0
                        """
                        cursor.execute(delete_sql)
                        conn.commit()

                        cursor.close()
                        conn.close()
                    except Exception:
                        pass

            except Exception:
                write_mysql_table(
                    df=stats_agg,
                    jdbc_url=self.jdbc_url,
                    table_name="mua_giai_doi_bong",
                    connection_properties=self.conn_props,
                    mode="append"
                )
                
                try:
                    import pymysql
                    db_host = os.getenv("MYSQL_HOST", "localhost")
                    db_port = int(os.getenv("MYSQL_PORT", "3306"))
                    db_user = os.getenv("MYSQL_USER", "root")
                    db_password = os.getenv("MYSQL_PASSWORD", "")
                    db_name = os.getenv("MYSQL_DATABASE", "defaultdb")
                    
                    conn = pymysql.connect(
                        host=db_host,
                        port=db_port,
                        user=db_user,
                        password=db_password,
                        database=db_name
                    )
                    cursor = conn.cursor()
                    
                    delete_sql = """
                        DELETE FROM mua_giai_doi_bong
                        WHERE so_tran_thang = 0 
                          AND so_tran_hoa = 0 
                          AND so_tran_thua = 0 
                          AND tong_ban_thang = 0 
                          AND tong_ban_thua = 0 
                          AND hieu_so = 0 
                          AND tong_diem = 0
                    """
                    cursor.execute(delete_sql)
                    conn.commit()
                    cursor.close()
                    conn.close()
                except Exception:
                    pass
                return

        except Exception:
            pass

    def _load_match_stats(self, matches_df: DataFrame, team_ids: Dict[str, int]):
        """Load vào bảng thong_ke_tran_dau - tham khảo code mới"""
        if not self._table_exists("thong_ke_tran_dau"):
            return

        if not self._table_exists("ket_qua_tran_dau"):
            return

        try:
            # Đọc ket_qua_tran_dau và join với tran_dau để lấy match_id
            # ket_qua_tran_dau.id_tran_dau là FK đến tran_dau.id, không phải match_id
            tran_dau_df = self.spark.read.jdbc(
                url=self.jdbc_url,
                table="tran_dau",
                properties=self.conn_props
            ).select(
                col("id").alias("tran_dau_id"),
                col("id_tran_dau").alias("match_id")
            )
            
            ket_qua_df = self.spark.read.jdbc(
                url=self.jdbc_url,
                table="ket_qua_tran_dau",
                properties=self.conn_props
            ).select(
                col("id").alias("ket_qua_id"),
                col("id_tran_dau").alias("ket_qua_tran_dau_id"),
                col("id_doi").alias("ket_qua_id_doi"),
                col("la_doi_nha").alias("ket_qua_la_doi_nha")
            )
            
            # Join với tran_dau để lấy match_id từ tran_dau.id_tran_dau
            ket_qua_df = ket_qua_df.join(
                broadcast(tran_dau_df),
                ket_qua_df["ket_qua_tran_dau_id"] == tran_dau_df["tran_dau_id"],
                "inner"
            ).select(
                col("ket_qua_id"),
                col("match_id").alias("ket_qua_match_id"),
                col("ket_qua_id_doi"),
                col("ket_qua_la_doi_nha")
            )
        except Exception:
            return

        available_cols = set(matches_df.columns)
        
        def create_stats_df(is_home: bool):
            """Tạo DataFrame stats cho home hoặc away team - tham khảo code mới"""
            prefix = "home_" if is_home else "away_"
            team_col = "home_team" if is_home else "away_team"
            
            # Mapping với danh sách các tên cột có thể có (theo thứ tự ưu tiên)
            # Lưu ý: Tên cột trong S3 được tạo từ stat_name với logic: stat_name.lower().replace(" ", "_").replace("%", "pct")
            mapping = {
                "id_tran_dau": ["match_id"],
                "team_name": [team_col],
                "ty_le_so_huu_banh": [
                    f"{prefix}possession_pct", f"{prefix}possession", 
                    f"possession_{prefix.rstrip('_')}_pct", f"possession_{prefix.rstrip('_')}",
                    f"{prefix}possession_percent", f"possession_percent_{prefix.rstrip('_')}"
                ],
                "tong_so_cu_sut": [f"{prefix}shots", f"{prefix}total_shots", f"shots_{prefix.rstrip('_')}", f"total_shots_{prefix.rstrip('_')}"],
                "cu_sut_trung_dich": [f"{prefix}shots_on_target", f"shots_on_target_{prefix.rstrip('_')}"],
                "cu_sut_ngoai_dich": [f"{prefix}shots_off_target", f"shots_off_target_{prefix.rstrip('_')}"],
                "cu_sut_trong_vong_cam": [f"{prefix}shots_inside_the_box", f"{prefix}shots_inside_box", f"shots_inside_box_{prefix.rstrip('_')}"],
                "cu_sut_ngoai_vong_cam": [f"{prefix}shots_outside_the_box", f"{prefix}shots_outside_box", f"shots_outside_box_{prefix.rstrip('_')}"],
                "tong_so_duong_chuyen": [f"{prefix}passes", f"{prefix}total_passes", f"passes_{prefix.rstrip('_')}", f"total_passes_{prefix.rstrip('_')}"],
                "ty_le_duong_chuyen_thanh_cong": [
                    f"{prefix}pass_accuracy_pct", f"{prefix}pass_accuracy", 
                    f"{prefix}total_passes_pct", f"{prefix}passes_pct",
                    f"pass_accuracy_{prefix.rstrip('_')}_pct", f"pass_accuracy_{prefix.rstrip('_')}",
                    f"total_passes_pct_{prefix.rstrip('_')}", f"passes_pct_{prefix.rstrip('_')}"
                ],
                "so_duong_chuyen_dai_thanh_cong": [
                    f"{prefix}long_passes_completed", f"{prefix}long_passes", 
                    f"long_passes_completed_{prefix.rstrip('_')}", f"long_passes_{prefix.rstrip('_')}",
                    f"{prefix}long_passes_successful"
                ],
                "ty_le_duong_chuyen_dai_thanh_cong": [
                    f"{prefix}long_passes_pct_completed", f"{prefix}long_pass_accuracy_pct", f"{prefix}long_pass_accuracy",
                    f"long_passes_pct_completed_{prefix.rstrip('_')}", f"long_pass_accuracy_{prefix.rstrip('_')}_pct",
                    f"long_pass_accuracy_{prefix.rstrip('_')}"
                ],
                "so_duong_chuyen_ngang_thanh_cong": [
                    f"{prefix}crosses_completed", f"{prefix}total_crosses_completed", f"{prefix}crosses",
                    f"crosses_completed_{prefix.rstrip('_')}", f"total_crosses_completed_{prefix.rstrip('_')}",
                    f"crosses_{prefix.rstrip('_')}", f"{prefix}successful_crosses"
                ],
                "ty_le_duong_chuyen_ngang_thanh_cong": [
                    f"{prefix}cross_accuracy_pct", f"{prefix}cross_accuracy", 
                    f"{prefix}total_crosses_pct_completed", f"{prefix}crosses_pct",
                    f"cross_accuracy_{prefix.rstrip('_')}_pct", f"cross_accuracy_{prefix.rstrip('_')}",
                    f"total_crosses_pct_completed_{prefix.rstrip('_')}"
                ],
                "so_duong_chuyen_xuyen_phong": [f"{prefix}through_balls", f"through_balls_{prefix.rstrip('_')}", f"{prefix}through_balls_completed"],
                "ty_le_duong_chuyen_xuyen_phong": [
                    f"{prefix}through_balls_pct", f"{prefix}through_ball_accuracy_pct", f"{prefix}through_ball_accuracy",
                    f"through_balls_pct_{prefix.rstrip('_')}", f"through_ball_accuracy_{prefix.rstrip('_')}_pct",
                    f"through_ball_accuracy_{prefix.rstrip('_')}"
                ],
                "tong_so_pha_re_bong": [f"{prefix}dribbles", f"{prefix}total_dribbles", f"dribbles_{prefix.rstrip('_')}"],
                "pha_bong_thanh_cong": [f"{prefix}successful_dribbles", f"{prefix}dribbles_won", f"dribbles_won_{prefix.rstrip('_')}"],
                "so_lan_chan_bong": [f"{prefix}touches", f"touches_{prefix.rstrip('_')}"],
                "so_lan_chan_bong_trong_vong_cam_doi_thu": [f"{prefix}touches_in_the_opposition_box", f"{prefix}touches_in_box", f"touches_in_box_{prefix.rstrip('_')}"],
                "pha_bong_thang": [f"{prefix}tackles_won", f"{prefix}duels_won", f"duels_won_{prefix.rstrip('_')}"],
                "ty_le_pha_bong_thang": [f"{prefix}tackles_won_pct", f"{prefix}duels_won_pct", f"duels_won_pct_{prefix.rstrip('_')}"],
                "tran_chap_thang": [f"{prefix}duels_won", f"duels_won_{prefix.rstrip('_')}"],
                "tran_chap_khong_trung_thang": [f"{prefix}aerial_duels_won", f"{prefix}duels_lost", f"duels_lost_{prefix.rstrip('_')}", f"aerial_duels_won_{prefix.rstrip('_')}"],
                "chan_bong": [f"{prefix}interceptions", f"interceptions_{prefix.rstrip('_')}"],
                "pha_bong_ra": [f"{prefix}clearances", f"clearances_{prefix.rstrip('_')}"],
                "chan_cu_sut": [f"{prefix}blocks", f"blocks_{prefix.rstrip('_')}"],
                "cu_cuu": [f"{prefix}saves", f"saves_{prefix.rstrip('_')}"],
                "so_phat_goc": [f"{prefix}corners", f"corners_{prefix.rstrip('_')}"],
                "so_vi_vi": [f"{prefix}offsides", f"offsides_{prefix.rstrip('_')}"],
                "so_loi_pham": [f"{prefix}fouls", f"fouls_{prefix.rstrip('_')}"],
                "so_the_vang": [f"{prefix}yellow_cards", f"yellow_cards_{prefix.rstrip('_')}"],
                "so_the_do": [f"{prefix}red_cards", f"red_cards_{prefix.rstrip('_')}"],
                "co_hoi_lon": [f"{prefix}big_chances", f"big_chances_{prefix.rstrip('_')}"],
                "co_hoi_lon_tao_ra": [f"{prefix}big_chances_created", f"big_chances_created_{prefix.rstrip('_')}"],
                "danh_trung_khung_thanh": [f"{prefix}hit_woodwork", f"hit_woodwork_{prefix.rstrip('_')}"],
                "xg": [f"{prefix}xg", f"xg_{prefix.rstrip('_')}", f"{prefix}expected_goals"],
                "quang_duong_di_chuyen": [
                    f"{prefix}distance_covered", f"distance_covered_{prefix.rstrip('_')}",
                    f"{prefix}distance_covered_km", f"distance_covered_km_{prefix.rstrip('_')}",
                    f"{prefix}total_distance", f"total_distance_{prefix.rstrip('_')}"
                ]
            }
            
            # Hàm tìm tên cột từ danh sách các tên có thể có
            def find_col_name(possible_names):
                for name in possible_names:
                    if name in available_cols:
                        return name
                return None
            
            # Hàm parse và normalize giá trị percentage (xử lý string có "%" hoặc decimal 0-1)
            def parse_percentage(col_expr):
                # Thử cast trực tiếp sang Decimal
                numeric_val = col_expr.cast(DecimalType(10, 2))
                # Nếu là string có "%", thử parse (loại bỏ "%")
                string_cleaned = regexp_replace(col_expr.cast(StringType()), "%", "")
                string_val = string_cleaned.cast(DecimalType(10, 2))
                # Xử lý: nếu giá trị <= 1, nhân với 100 (decimal format 0-1 -> 0-100)
                # Nếu giá trị > 1 và <= 100, giữ nguyên (đã là phần trăm)
                # Ưu tiên numeric_val, nếu NULL thì dùng string_val
                result = when(numeric_val.isNotNull(), numeric_val).otherwise(string_val)
                return when(
                    result.isNotNull(),
                    when(result <= 1, result * 100)
                    .when((result > 1) & (result <= 100), result)
                    .otherwise(None)
                ).otherwise(None)
            
            # Hàm parse distance (có thể là km hoặc m)
            def parse_distance(col_expr):
                # Cast sang Decimal
                numeric_val = col_expr.cast(DecimalType(10, 2))
                # Nếu giá trị > 1000, có thể là m, chia cho 1000 để chuyển sang km
                # Nếu giá trị <= 1000, giả sử là km
                return when(
                    numeric_val.isNotNull() & (numeric_val >= 0),
                    when(numeric_val > 1000, numeric_val / 1000)
                    .otherwise(numeric_val)
                ).otherwise(None)
            
            # Tạo select expressions - chỉ chọn các cột tồn tại
            select_exprs = []
            for target_col, possible_names in mapping.items():
                source_col = find_col_name(possible_names)
                if source_col:
                    if target_col == "id_tran_dau":
                        select_exprs.append(col(source_col).alias(target_col))
                    elif target_col == "team_name":
                        select_exprs.append(col(source_col).alias(target_col))
                    elif target_col == "ty_le_so_huu_banh":
                        # Xử lý đặc biệt cho possession - có thể là 0-1 hoặc 0-100
                        parsed_val = parse_percentage(col(source_col))
                        select_exprs.append(
                            when(parsed_val.isNotNull() & (parsed_val >= 0) & (parsed_val <= 100),
                                 parsed_val).otherwise(None).cast(DecimalType(5, 2)).alias(target_col)
                        )
                    elif "ty_le" in target_col:
                        # Xử lý các tỷ lệ khác
                        parsed_val = parse_percentage(col(source_col))
                        select_exprs.append(
                            when(parsed_val.isNotNull() & (parsed_val >= 0) & (parsed_val <= 100),
                                 parsed_val).otherwise(None).cast(DecimalType(5, 2)).alias(target_col)
                        )
                    elif target_col == "quang_duong_di_chuyen":
                        # Xử lý distance - có thể là km hoặc m
                        parsed_val = parse_distance(col(source_col))
                        select_exprs.append(
                            when(parsed_val.isNotNull() & (parsed_val >= 0),
                                 parsed_val).otherwise(None).cast(DecimalType(6, 2)).alias(target_col)
                        )
                    elif target_col == "xg":
                        select_exprs.append(
                            when(col(source_col).isNotNull() & (col(source_col) >= 0),
                                 col(source_col)).otherwise(None).cast(DecimalType(6, 2)).alias(target_col)
                        )
                    else:
                        # Các cột số nguyên
                        select_exprs.append(
                            when(col(source_col).isNotNull() & (col(source_col) >= 0),
                                 col(source_col)).otherwise(None).cast(IntegerType()).alias(target_col)
                        )
                else:
                    # Thêm NULL nếu không tìm thấy cột nào
                    if "ty_le" in target_col or target_col in ["xg", "quang_duong_di_chuyen"]:
                        if target_col == "xg" or target_col == "quang_duong_di_chuyen":
                            select_exprs.append(lit(None).cast(DecimalType(6, 2)).alias(target_col))
                        else:
                            select_exprs.append(lit(None).cast(DecimalType(5, 2)).alias(target_col))
                    else:
                        select_exprs.append(lit(None).cast(IntegerType()).alias(target_col))
            
            stats_df = matches_df.select(*select_exprs).filter(col("id_tran_dau").isNotNull())
            
            if self._is_empty(stats_df):
                return None
            
            # Join với teams_lookup để lấy team_id
            teams_lookup = self.spark.createDataFrame(
                [(name, tid) for name, tid in team_ids.items()],
                ["team_name", "team_id"]
            )
            
            stats_with_team = stats_df.join(
                broadcast(teams_lookup),
                "team_name",
                "inner"
            )
            
            if self._is_empty(stats_with_team):
                return None
            
            # Join với ket_qua_tran_dau để lấy id_ket_qua
            # Sử dụng match_id và team_id để join
            stats_final = stats_with_team.join(
                broadcast(ket_qua_df.alias("kq")),
                (stats_with_team["id_tran_dau"] == col("kq.ket_qua_match_id")) &
                (stats_with_team["team_id"] == col("kq.ket_qua_id_doi")) &
                (col("kq.ket_qua_la_doi_nha") == (1 if is_home else 0)),
                "inner"
            )
            
            if self._is_empty(stats_final):
                return None
            
            # Select các cột cần thiết
            stats_final = stats_final.select(
                col("kq.ket_qua_id").alias("id_ket_qua"),
                *[col(c) for c in stats_df.columns if c not in ["id_tran_dau", "team_name"]]
            )
            
            shots_col = col("tong_so_cu_sut")
            shots_on_target_col = col("cu_sut_trung_dich")
            shots_off_target_col = col("cu_sut_ngoai_dich")
            duels_won_col = col("pha_bong_thang")
            tran_chap_thang_col = col("tran_chap_thang")
            tran_chap_khong_trung_thang_col = col("tran_chap_khong_trung_thang")
            duels_total_col = when(tran_chap_thang_col.isNotNull() & tran_chap_khong_trung_thang_col.isNotNull(),
                                   tran_chap_thang_col + tran_chap_khong_trung_thang_col).otherwise(None)
            
            return stats_final.select(
                col("id_ket_qua"),
                col("ty_le_so_huu_banh"),
                shots_col.alias("tong_so_cu_sut"),
                shots_on_target_col.alias("cu_sut_trung_dich"),
                # Ưu tiên dùng cột shots_off_target nếu có, nếu không thì tính từ tổng - trúng đích
                when(shots_off_target_col.isNotNull(), shots_off_target_col)
                 .when(shots_col.isNotNull() & shots_on_target_col.isNotNull(), shots_col - shots_on_target_col)
                 .otherwise(lit(None)).cast(IntegerType()).alias("cu_sut_ngoai_dich"),
                col("cu_sut_trong_vong_cam"),
                col("cu_sut_ngoai_vong_cam"),
                col("tong_so_duong_chuyen"),
                col("ty_le_duong_chuyen_thanh_cong"),
                col("so_duong_chuyen_dai_thanh_cong"),
                col("ty_le_duong_chuyen_dai_thanh_cong"),
                col("so_duong_chuyen_ngang_thanh_cong"),
                col("ty_le_duong_chuyen_ngang_thanh_cong"),
                col("so_duong_chuyen_xuyen_phong"),
                col("ty_le_duong_chuyen_xuyen_phong"),
                col("tong_so_pha_re_bong"),
                col("pha_bong_thanh_cong"),
                col("so_lan_chan_bong"),
                col("so_lan_chan_bong_trong_vong_cam_doi_thu"),
                duels_won_col.alias("pha_bong_thang"),
                when(duels_total_col.isNotNull() & (duels_total_col > 0) & duels_won_col.isNotNull(),
                     (duels_won_col / duels_total_col) * 100)
                 .otherwise(col("ty_le_pha_bong_thang")).cast(DecimalType(5, 2)).alias("ty_le_pha_bong_thang"),
                tran_chap_thang_col.alias("tran_chap_thang"),
                when(duels_total_col.isNotNull() & duels_won_col.isNotNull(), duels_total_col - duels_won_col)
                 .otherwise(tran_chap_khong_trung_thang_col).cast(IntegerType()).alias("tran_chap_khong_trung_thang"),
                col("chan_bong"),
                col("pha_bong_ra"),
                col("chan_cu_sut"),
                col("cu_cuu"),
                col("so_phat_goc"),
                col("so_vi_vi"),
                col("so_loi_pham"),
                col("so_the_vang"),
                col("so_the_do"),
                col("co_hoi_lon"),
                col("co_hoi_lon_tao_ra"),
                col("danh_trung_khung_thanh"),
                col("xg"),
                col("quang_duong_di_chuyen")
            )

        home_stats = create_stats_df(True)
        away_stats = create_stats_df(False)
        
        if home_stats is None:
            home_stats = self.spark.createDataFrame([], schema=None)
        if away_stats is None:
            away_stats = self.spark.createDataFrame([], schema=None)
        
        if self._is_empty(home_stats) and self._is_empty(away_stats):
            return
        
        all_stats = home_stats.union(away_stats).dropDuplicates(["id_ket_qua"])

        if self._is_empty(all_stats):
            return

        # Tách ra các bản ghi mới và các bản ghi cần update
        new_stats = None
        update_stats = None
        
        try:
            existing_df = self.spark.read.jdbc(
                url=self.jdbc_url,
                table="(SELECT id_ket_qua FROM thong_ke_tran_dau) AS existing",
                properties=self.conn_props
            )
            if not self._is_empty(existing_df):
                # Bản ghi mới (chưa tồn tại)
                new_stats = all_stats.join(
                    broadcast(existing_df),
                    all_stats["id_ket_qua"] == existing_df["id_ket_qua"],
                    "left_anti"
                )
                # Bản ghi cần update (đã tồn tại)
                update_stats = all_stats.join(
                    broadcast(existing_df),
                    all_stats["id_ket_qua"] == existing_df["id_ket_qua"],
                    "inner"
                )
            else:
                new_stats = all_stats
        except Exception:
            new_stats = all_stats

        if new_stats is not None and not self._is_empty(new_stats):
            write_mysql_table(
                df=new_stats,
                jdbc_url=self.jdbc_url,
                table_name="thong_ke_tran_dau",
                connection_properties=self.conn_props,
                mode="append"
            )

        if update_stats is not None and not self._is_empty(update_stats):
            try:
                import pymysql
            except ImportError:
                return

            db_host = os.getenv("MYSQL_HOST", "localhost")
            db_port = int(os.getenv("MYSQL_PORT", "3306"))
            db_user = os.getenv("MYSQL_USER", "root")
            db_password = os.getenv("MYSQL_PASSWORD", "")
            db_name = os.getenv("MYSQL_DATABASE", "defaultdb")

            try:
                update_records = update_stats.collect()
                conn = pymysql.connect(
                    host=db_host,
                    port=db_port,
                    user=db_user,
                    password=db_password,
                    database=db_name
                )
                cursor = conn.cursor()

                update_sql = """
                    UPDATE thong_ke_tran_dau
                    SET ty_le_so_huu_banh = %s,
                        tong_so_cu_sut = %s,
                        cu_sut_trung_dich = %s,
                        cu_sut_ngoai_dich = %s,
                        cu_sut_trong_vong_cam = %s,
                        cu_sut_ngoai_vong_cam = %s,
                        tong_so_duong_chuyen = %s,
                        ty_le_duong_chuyen_thanh_cong = %s,
                        so_duong_chuyen_dai_thanh_cong = %s,
                        ty_le_duong_chuyen_dai_thanh_cong = %s,
                        so_duong_chuyen_ngang_thanh_cong = %s,
                        ty_le_duong_chuyen_ngang_thanh_cong = %s,
                        so_duong_chuyen_xuyen_phong = %s,
                        ty_le_duong_chuyen_xuyen_phong = %s,
                        tong_so_pha_re_bong = %s,
                        pha_bong_thanh_cong = %s,
                        so_lan_chan_bong = %s,
                        so_lan_chan_bong_trong_vong_cam_doi_thu = %s,
                        pha_bong_thang = %s,
                        ty_le_pha_bong_thang = %s,
                        tran_chap_thang = %s,
                        tran_chap_khong_trung_thang = %s,
                        chan_bong = %s,
                        pha_bong_ra = %s,
                        chan_cu_sut = %s,
                        cu_cuu = %s,
                        so_phat_goc = %s,
                        so_vi_vi = %s,
                        so_loi_pham = %s,
                        so_the_vang = %s,
                        so_the_do = %s,
                        co_hoi_lon = %s,
                        co_hoi_lon_tao_ra = %s,
                        danh_trung_khung_thanh = %s,
                        xg = %s,
                        quang_duong_di_chuyen = %s
                    WHERE id_ket_qua = %s
                """

                update_data = [
                    (
                        float(row.ty_le_so_huu_banh) if row.ty_le_so_huu_banh is not None else None,
                        int(row.tong_so_cu_sut) if row.tong_so_cu_sut is not None else None,
                        int(row.cu_sut_trung_dich) if row.cu_sut_trung_dich is not None else None,
                        int(row.cu_sut_ngoai_dich) if row.cu_sut_ngoai_dich is not None else None,
                        int(row.cu_sut_trong_vong_cam) if row.cu_sut_trong_vong_cam is not None else None,
                        int(row.cu_sut_ngoai_vong_cam) if row.cu_sut_ngoai_vong_cam is not None else None,
                        int(row.tong_so_duong_chuyen) if row.tong_so_duong_chuyen is not None else None,
                        float(row.ty_le_duong_chuyen_thanh_cong) if row.ty_le_duong_chuyen_thanh_cong is not None else None,
                        int(row.so_duong_chuyen_dai_thanh_cong) if row.so_duong_chuyen_dai_thanh_cong is not None else None,
                        float(row.ty_le_duong_chuyen_dai_thanh_cong) if row.ty_le_duong_chuyen_dai_thanh_cong is not None else None,
                        int(row.so_duong_chuyen_ngang_thanh_cong) if row.so_duong_chuyen_ngang_thanh_cong is not None else None,
                        float(row.ty_le_duong_chuyen_ngang_thanh_cong) if row.ty_le_duong_chuyen_ngang_thanh_cong is not None else None,
                        int(row.so_duong_chuyen_xuyen_phong) if row.so_duong_chuyen_xuyen_phong is not None else None,
                        float(row.ty_le_duong_chuyen_xuyen_phong) if row.ty_le_duong_chuyen_xuyen_phong is not None else None,
                        int(row.tong_so_pha_re_bong) if row.tong_so_pha_re_bong is not None else None,
                        int(row.pha_bong_thanh_cong) if row.pha_bong_thanh_cong is not None else None,
                        int(row.so_lan_chan_bong) if row.so_lan_chan_bong is not None else None,
                        int(row.so_lan_chan_bong_trong_vong_cam_doi_thu) if row.so_lan_chan_bong_trong_vong_cam_doi_thu is not None else None,
                        int(row.pha_bong_thang) if row.pha_bong_thang is not None else None,
                        float(row.ty_le_pha_bong_thang) if row.ty_le_pha_bong_thang is not None else None,
                        int(row.tran_chap_thang) if row.tran_chap_thang is not None else None,
                        int(row.tran_chap_khong_trung_thang) if row.tran_chap_khong_trung_thang is not None else None,
                        int(row.chan_bong) if row.chan_bong is not None else None,
                        int(row.pha_bong_ra) if row.pha_bong_ra is not None else None,
                        int(row.chan_cu_sut) if row.chan_cu_sut is not None else None,
                        int(row.cu_cuu) if row.cu_cuu is not None else None,
                        int(row.so_phat_goc) if row.so_phat_goc is not None else None,
                        int(row.so_vi_vi) if row.so_vi_vi is not None else None,
                        int(row.so_loi_pham) if row.so_loi_pham is not None else None,
                        int(row.so_the_vang) if row.so_the_vang is not None else None,
                        int(row.so_the_do) if row.so_the_do is not None else None,
                        int(row.co_hoi_lon) if row.co_hoi_lon is not None else None,
                        int(row.co_hoi_lon_tao_ra) if row.co_hoi_lon_tao_ra is not None else None,
                        int(row.danh_trung_khung_thanh) if row.danh_trung_khung_thanh is not None else None,
                        float(row.xg) if row.xg is not None else None,
                        float(row.quang_duong_di_chuyen) if row.quang_duong_di_chuyen is not None else None,
                        int(row.id_ket_qua)
                    )
                    for row in update_records
                ]

                cursor.executemany(update_sql, update_data)
                conn.commit()
                cursor.close()
                conn.close()
            except Exception:
                pass
    def _load_lineups(self, matches_df: DataFrame, team_ids: Dict[str, int]):
        available_columns = set(matches_df.columns)

        if not self._table_exists("doi_hinh_tran_dau"):
            return

        try:
            tran_dau_df = self.spark.read.jdbc(
                url=self.jdbc_url,
                table="tran_dau",
                properties=self.conn_props
            ).select("id", "id_tran_dau").alias("td")
        except Exception:
            return

        teams_lookup = self.spark.createDataFrame(
            [(name, tid) for name, tid in team_ids.items()],
            ["team_name", "id_doi_bong"]
        )

        def create_lineup_df(is_home: bool):
            prefix = "home_" if is_home else "away_"
            team_col = "home_team" if is_home else "away_team"

            select_exprs = [
                col("match_id").alias("id_tran_dau"),
                col(team_col).alias("team_name")
            ]

            formation_col = f"{prefix}formation"
            if formation_col in available_columns:
                select_exprs.append(col(formation_col).alias("doi_hinh"))
            else:
                select_exprs.append(lit(None).cast(StringType()).alias("doi_hinh"))

            starting_xi_col = f"{prefix}starting_xi"
            if starting_xi_col in available_columns:
                select_exprs.append(col(starting_xi_col).alias("doi_hinh_xuat_phat"))
            else:
                select_exprs.append(lit(None).cast(StringType()).alias("doi_hinh_xuat_phat"))

            substitutes_col = f"{prefix}substitutes"
            if substitutes_col in available_columns:
                select_exprs.append(col(substitutes_col).alias("cau_thu_du_bi"))
            else:
                select_exprs.append(lit(None).cast(StringType()).alias("cau_thu_du_bi"))

            select_exprs.append(lit(None).cast(IntegerType()).alias("vi_tri_bang_xep_hang"))

            return matches_df.select(*select_exprs).filter(
                col("match_id").isNotNull() & col(team_col).isNotNull()
            )

        home_lineups = create_lineup_df(True)
        away_lineups = create_lineup_df(False)
        all_lineups = home_lineups.union(away_lineups)

        lineups_with_match = all_lineups.join(
            broadcast(tran_dau_df),
            all_lineups["id_tran_dau"] == col("td.id_tran_dau"),
            "inner"
        )

        lineups_with_team = lineups_with_match.join(
            broadcast(teams_lookup),
            lineups_with_match["team_name"] == teams_lookup["team_name"],
            "inner"
        )

        lineups_final = lineups_with_team.select(
            col("td.id").alias("id_tran_dau"),
            col("id_doi_bong"),
            col("doi_hinh"),
            col("vi_tri_bang_xep_hang"),
            col("doi_hinh_xuat_phat"),
            col("cau_thu_du_bi")
        ).dropDuplicates(["id_tran_dau", "id_doi_bong"])

        if not self._is_empty(lineups_final):
            try:
                existing_df = self.spark.read.jdbc(
                    url=self.jdbc_url,
                    table="(SELECT id_tran_dau, id_doi_bong FROM doi_hinh_tran_dau) AS existing",
                    properties=self.conn_props
                )
                if not self._is_empty(existing_df):
                    existing_keys = existing_df.select("id_tran_dau", "id_doi_bong")
                    lineups_final = lineups_final.join(
                        broadcast(existing_keys),
                        ["id_tran_dau", "id_doi_bong"],
                        "left_anti"
                    )
            except Exception:
                pass

        if not self._is_empty(lineups_final):
            write_mysql_table(
                df=lineups_final,
                jdbc_url=self.jdbc_url,
                table_name="doi_hinh_tran_dau",
                connection_properties=self.conn_props,
                mode="append"
            )

    def _load_goals(self, matches_df: DataFrame, team_ids: Dict[str, int]):
        pass

    def _load_players_master(self, players_df: DataFrame):
        if "player_name" not in players_df.columns:
            return

        players_master = players_df.select(
            col("player_name").alias("ten_cau_thu")
        ).filter(
            col("ten_cau_thu").isNotNull()
        ).dropDuplicates(["ten_cau_thu"])

        if not self._is_empty(players_master):
            try:
                existing_df = self.spark.read.jdbc(
                    url=self.jdbc_url,
                    table="(SELECT DISTINCT ten_cau_thu FROM cau_thu) AS existing",
                    properties=self.conn_props
                )
                if not self._is_empty(existing_df):
                    players_master = players_master.join(
                        broadcast(existing_df),
                        players_master["ten_cau_thu"] == existing_df["ten_cau_thu"],
                        "left_anti"
                    )
            except:
                pass

        if not self._is_empty(players_master):
            write_mysql_table(
                df=players_master,
                jdbc_url=self.jdbc_url,
                table_name="cau_thu",
                connection_properties=self.conn_props,
                mode="append"
            )

    def _load_players_match(self, players_df: DataFrame):
        try:
            tran_dau_df = self.spark.read.jdbc(
                url=self.jdbc_url,
                table="tran_dau",
                properties=self.conn_props
            ).select("id", "id_tran_dau").alias("td")
        except:
            return

        try:
            doi_bong_df = self.spark.read.jdbc(
                url=self.jdbc_url,
                table="doi_bong",
                properties=self.conn_props
            ).select("id", "ten_doi").alias("db")
        except:
            return

        available_cols = set(players_df.columns)

        match_id_col = None
        for col_name in ["match_id", "matchid", "id_tran_dau"]:
            if col_name in available_cols:
                match_id_col = col_name
                break

        team_col = None
        for col_name in ["team", "team_name", "doi_bong", "ten_doi"]:
            if col_name in available_cols:
                team_col = col_name
                break

        player_name_col = None
        for col_name in ["player_name", "name", "ten_cau_thu", "player"]:
            if col_name in available_cols:
                player_name_col = col_name
                break

        if not match_id_col or not team_col or not player_name_col:
            return

        select_exprs = [
            col(match_id_col).alias("match_id"),
            col(team_col).alias("team_name"),
            col(player_name_col).alias("ten_cau_thu")
        ]

        if "position" in available_cols:
            select_exprs.append(col("position").alias("vi_tri"))
        else:
            select_exprs.append(lit(None).cast(StringType()).alias("vi_tri"))

        if "position" in available_cols:
            select_exprs.append(
                when(col("position").isNotNull() &
                     (upper(trim(col("position"))).like("%STARTING%")),
                     lit(True)).otherwise(lit(False)).cast(BooleanType()).alias("la_doi_hinh_xuat_phat")
            )
        else:
            select_exprs.append(lit(False).cast(BooleanType()).alias("la_doi_hinh_xuat_phat"))

        players_match = players_df.select(*select_exprs).filter(
            col("match_id").isNotNull() &
            col("team_name").isNotNull() &
            col("ten_cau_thu").isNotNull()
        )

        try:
            cau_thu_df = self.spark.read.jdbc(
                url=self.jdbc_url,
                table="cau_thu",
                properties=self.conn_props
            ).select("id", "ten_cau_thu").alias("ct")
        except:
            cau_thu_df = None

        players_with_ids = players_match.join(
            broadcast(tran_dau_df),
            players_match["match_id"] == col("td.id_tran_dau"),
            "inner"
        ).join(
            broadcast(doi_bong_df),
            players_match["team_name"] == col("db.ten_doi"),
            "inner"
        )

        if cau_thu_df is not None:
            players_selected = players_with_ids.select(
                col("td.id").alias("id_tran_dau"),
                col("db.id").alias("id_doi_bong"),
                col("ten_cau_thu"),
                col("vi_tri"),
                col("la_doi_hinh_xuat_phat")
            ).alias("players")

            players_match_final = players_selected.join(
                broadcast(cau_thu_df),
                col("players.ten_cau_thu") == col("ct.ten_cau_thu"),
                "left"
            ).select(
                col("players.id_tran_dau").alias("id_tran_dau"),
                col("players.id_doi_bong").alias("id_doi_bong"),
                col("ct.id").alias("id_cau_thu"),
                col("players.ten_cau_thu").alias("ten_cau_thu"),
                col("players.vi_tri").alias("vi_tri"),
                col("players.la_doi_hinh_xuat_phat").alias("la_doi_hinh_xuat_phat")
            ).dropDuplicates(["id_tran_dau", "id_doi_bong", "ten_cau_thu"])
        else:
            players_match_final = players_with_ids.select(
                col("td.id").alias("id_tran_dau"),
                col("db.id").alias("id_doi_bong"),
                lit(None).cast(LongType()).alias("id_cau_thu"),
                col("ten_cau_thu"),
                col("vi_tri"),
                col("la_doi_hinh_xuat_phat")
            ).dropDuplicates(["id_tran_dau", "id_doi_bong", "ten_cau_thu"])

        if not self._is_empty(players_match_final):
            try:
                existing_df = self.spark.read.jdbc(
                    url=self.jdbc_url,
                    table="(SELECT id_tran_dau, id_doi_bong, ten_cau_thu FROM cau_thu_tran_dau) AS existing",
                    properties=self.conn_props
                )
                if not self._is_empty(existing_df):
                    existing_keys = existing_df.select("id_tran_dau", "id_doi_bong", "ten_cau_thu")
                    players_match_final = players_match_final.join(
                        broadcast(existing_keys),
                        ["id_tran_dau", "id_doi_bong", "ten_cau_thu"],
                        "left_anti"
                    )
            except:
                pass

        if not self._is_empty(players_match_final):
            write_mysql_table(
                df=players_match_final,
                jdbc_url=self.jdbc_url,
                table_name="cau_thu_tran_dau",
                connection_properties=self.conn_props,
                mode="append"
            )

    def load_players_data(self, players_df: DataFrame):
        if players_df is None or self._is_empty(players_df):
            return

        if self._table_exists("cau_thu"):
            self._load_players_master(players_df)

        if self._table_exists("cau_thu_tran_dau"):
            self._load_players_match(players_df)

    def load_all_gold_data(self, season: str = None):
        files = self.read_gold_files(season=season)

        if files.get("matches") is not None:
            self.load_matches_data(files["matches"])

        if files.get("players") is not None:
            self.load_players_data(files["players"])

        if files.get("events") is not None:
            self.load_events_data(files["events"])

    def load_events_data(self, events_df: DataFrame):
        if events_df is None or self._is_empty(events_df):
            return

        if not self._table_exists("su_kien_tran_dau"):
            return

        try:
            tran_dau_df = self.spark.read.jdbc(
                url=self.jdbc_url,
                table="tran_dau",
                properties=self.conn_props
            ).select("id", "id_tran_dau").alias("td")
        except:
            return

        try:
            doi_bong_df = self.spark.read.jdbc(
                url=self.jdbc_url,
                table="doi_bong",
                properties=self.conn_props
            ).select("id", "ten_doi").alias("db")
        except:
            return

        available_cols = set(events_df.columns)

        match_id_col = None
        for col_name in ["match_id", "matchid", "id_tran_dau"]:
            if col_name in available_cols:
                match_id_col = col_name
                break

        team_col = None
        for col_name in ["team", "team_name", "doi_bong", "ten_doi"]:
            if col_name in available_cols:
                team_col = col_name
                break

        event_type_col = None
        for col_name in ["event_type", "type", "loai_su_kien", "event"]:
            if col_name in available_cols:
                event_type_col = col_name
                break

        minute_col = None
        for col_name in ["minute", "phut", "time", "min"]:
            if col_name in available_cols:
                minute_col = col_name
                break

        player_col = None
        for col_name in ["player", "player_name", "ten_cau_thu", "name"]:
            if col_name in available_cols:
                player_col = col_name
                break

        if not match_id_col or not team_col or not event_type_col:
            return

        select_exprs = [
            col(match_id_col).alias("match_id"),
            col(team_col).alias("team_name"),
            col(event_type_col).alias("loai_su_kien")
        ]

        if minute_col:
            select_exprs.append(
                when(col(minute_col).isNotNull() & (col(minute_col) >= 0),
                     col(minute_col).cast(IntegerType())).otherwise(None).cast(IntegerType()).alias("phut")
            )
        else:
            select_exprs.append(lit(None).cast(IntegerType()).alias("phut"))

        if player_col:
            select_exprs.append(col(player_col).alias("ten_cau_thu"))
        else:
            select_exprs.append(lit(None).cast(StringType()).alias("ten_cau_thu"))

        detail_col = None
        for col_name in ["detail", "chi_tiet", "description", "info"]:
            if col_name in available_cols:
                detail_col = col_name
                break

        if detail_col:
            select_exprs.append(col(detail_col).alias("chi_tiet"))
        else:
            select_exprs.append(lit(None).cast(StringType()).alias("chi_tiet"))

        events_prep = events_df.select(*select_exprs).filter(
            col("match_id").isNotNull() &
            col("team_name").isNotNull() &
            col("loai_su_kien").isNotNull()
        )

        try:
            cau_thu_df = self.spark.read.jdbc(
                url=self.jdbc_url,
                table="cau_thu",
                properties=self.conn_props
            ).select("id", "ten_cau_thu").alias("ct")
        except:
            cau_thu_df = None

        events_with_ids = events_prep.join(
            broadcast(tran_dau_df),
            events_prep["match_id"] == col("td.id_tran_dau"),
            "inner"
        ).join(
            broadcast(doi_bong_df),
            events_prep["team_name"] == col("db.ten_doi"),
            "inner"
        )

        events_selected = events_with_ids.select(
            col("td.id").alias("id_tran_dau"),
            col("db.id").alias("id_doi_bong"),
            col("ten_cau_thu"),
            col("loai_su_kien"),
            col("phut"),
            col("chi_tiet")
        ).alias("events")

        if cau_thu_df is not None and player_col:
            events_final = events_selected.join(
                broadcast(cau_thu_df),
                col("events.ten_cau_thu") == col("ct.ten_cau_thu"),
                "left"
            ).select(
                col("events.id_tran_dau").alias("id_tran_dau"),
                col("events.id_doi_bong").alias("id_doi_bong"),
                col("ct.id").alias("id_cau_thu"),
                col("events.ten_cau_thu").alias("ten_cau_thu"),
                col("events.loai_su_kien").alias("loai_su_kien"),
                col("events.phut").alias("phut"),
                col("events.chi_tiet").alias("chi_tiet")
            )
        else:
            events_final = events_selected.select(
                col("events.id_tran_dau").alias("id_tran_dau"),
                col("events.id_doi_bong").alias("id_doi_bong"),
                lit(None).cast(LongType()).alias("id_cau_thu"),
                col("events.ten_cau_thu").alias("ten_cau_thu"),
                col("events.loai_su_kien").alias("loai_su_kien"),
                col("events.phut").alias("phut"),
                col("events.chi_tiet").alias("chi_tiet")
            )

        if not self._is_empty(events_final):
            write_mysql_table(
                df=events_final,
                jdbc_url=self.jdbc_url,
                table_name="su_kien_tran_dau",
                connection_properties=self.conn_props,
                mode="append"
            )

    def close(self):
        if self.streaming_query:
            self.streaming_query.stop()
        if self.spark:
            self.spark.stop()

    def __enter__(self):
        self.initialize()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Transform dữ liệu từ S3 Gold sang MySQL")
    parser.add_argument("--season", type=str, default=None, help="Mùa giải cụ thể (ví dụ: 2024-25)")
    parser.add_argument("--file-type", type=str, default=None,
                       choices=["matches", "players", "events", "analytics", "master"],
                       help="Loại file cụ thể để load")

    args = parser.parse_args()

    try:
        transformer = S3ToMySQLTransformer()
        transformer.initialize()

        if args.file_type:
            files = transformer.read_gold_files(season=args.season, file_type=args.file_type)
            if args.file_type == "matches":
                transformer.load_matches_data(files["matches"])
            elif args.file_type == "players":
                transformer.load_players_data(files["players"])
            elif args.file_type == "events":
                transformer.load_events_data(files["events"])
        else:
            transformer.load_all_gold_data(season=args.season)

        transformer.close()

    except Exception as e:
        import traceback
        traceback.print_exc()
        sys.exit(1)