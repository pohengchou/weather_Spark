# 0.匯入套件
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col,explode,lit,current_timestamp


# 從命令欄讀取動態參數(由airflow以bash傳遞)
if len(sys.argv)<3:
    #若傳入參數數量不足，顯示需傳入參數說明並退出
    print("Usage: clean_weather.py <date_path> <filename>")
    sys.exit(1)

DATE_PATH = sys.argv[1]
filename= sys.argv[2]
print(f"測試日期：{DATE_PATH}")

# 設定變數
STAGING_BUCKET_NAME = "ubike-471005-data-lake" 
# 原始 JSON 檔案路徑
GCS_WEATHER_INPUT_PATH = f"gs://{STAGING_BUCKET_NAME}/weather_raw/{DATE_PATH}/{filename}.json"
# 輸出路徑：使用 Parquet 格式
GCS_WEATHER_OUTPUT_PATH = f"gs://{STAGING_BUCKET_NAME}/weather_cleaned_parquet/{DATE_PATH}/"
print(f"使用的 Bucket 名稱: {STAGING_BUCKET_NAME}")
print(f"測試日期：{DATE_PATH}")
print(f"輸入路徑：{GCS_WEATHER_INPUT_PATH}")
print(f"輸出路徑：{GCS_WEATHER_OUTPUT_PATH}")


# 2.啟動 Spark Session
spark=SparkSession.builder.appName(f"WeatherETL_{DATE_PATH}").getOrCreate()
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
    .partitionBy("execution_date")\
    .parquet(GCS_WEATHER_OUTPUT_PATH)
print(f"數據處理完成，寫入路徑: {GCS_WEATHER_OUTPUT_PATH}")


# 7. 停止 Spark Session (Dataproc 叢集將被 Airflow 終止)
# 確保資源被釋放
spark.stop()
