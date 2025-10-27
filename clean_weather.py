# 0.匯入套件
from pyspark.sql import SparkSession
from pyspark.sql.functions import col,explode,lit,current_timestamp


# 1.設定變數
STAGING_BUCKET_NAME = "ubike-471005-data-lake" 
print(f"使用的 Bucket 名稱: {STAGING_BUCKET_NAME}")

# 使用固定的測試日期，以確保測試的穩定性
TEST_YEAR = "2025"
TEST_MONTH = "10"
TEST_DAY = "01"
DATE_PATH = f"{TEST_YEAR}/{TEST_MONTH}/{TEST_DAY}" # 2025/10/01
filename="20251001_000003"
print(f"測試日期：{DATE_PATH}")

# 原始 JSON 檔案路徑
GCS_WEATHER_INPUT_PATH = f"gs://{STAGING_BUCKET_NAME}/weather_raw/{DATE_PATH}/{filename}.json"
# 輸出路徑：使用 Parquet 格式
GCS_WEATHER_OUTPUT_PATH = f"gs://{STAGING_BUCKET_NAME}/weather_cleaned_parquet/{DATE_PATH}/"
print(f"輸入路徑：{GCS_WEATHER_INPUT_PATH}")
print(f"輸出路徑：{GCS_WEATHER_OUTPUT_PATH}")


# 2.啟動 Spark Session
spark=SparkSession.builder.appName("WeatherETLTesting").getOrCreate()
print("Spark Session 已經準備就緒，開始ETL流程。")


# 3.讀取原始 JSON檔案
df_raw=spark.read.json(GCS_WEATHER_INPUT_PATH,multiLine=True)
df_raw.printSchema()


# 4.展開嵌套的陣列 (攤平 Station 陣列)
# 展開目標路徑：'cwaopendata.dataset.Station'
# 將陣列中的每個元素變成一條新的記錄，並將其命名為 "station_data"
df_stations = df_raw.select(
    explode(col("cwaopendata.dataset.Station")).alias("station_data")
)
df_stations.printSchema()
print("\n數據展示 (前 2 條記錄的結構體):")
# 這裡使用 truncate=False 確保完整顯示結構體內容
df_stations.show(2, truncate=False)


# 5. 提取必要欄位並保留嵌套結構 (配合 dbt UNNEST 策略)
df_final = df_stations.select(
    # 頂層欄位 (dbt 中直接使用)
    col("station_data.StationId").alias("StationId"), 
    col("station_data.StationName").alias("StationName"),
    
    # 嵌套結構 (dbt 會使用點擊符號和 UNNEST)
    col("station_data.ObsTime").alias("ObsTime"), # 保持 STRUCT 類型 (包含 DateTime)
    col("station_data.GeoInfo").alias("GeoInfo"), # 保持 STRUCT 類型 (包含 Coordinates 陣列)
    col("station_data.WeatherElement").alias("WeatherElement"), # 保持 STRUCT 類型
    
    # 元數據 (Metadata)
    lit(DATE_PATH).alias("execution_date"),
    current_timestamp().alias("processed_at")
)
print("保留嵌套結構後的最終 Schema:")
df_final.printSchema()


# 6. 寫入 GCS (Parquet 格式)
df_final.write\
    .mode("overwrite")\
    .partitiony("execution_date")\
    .parquet(GCS_WEATHER_OUTPUT_PATH)