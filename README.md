# ⚡ weather_Spark: CWA 氣象數據預清理腳本

本儲存庫包含用於 Google Cloud Platform (GCP) ELT 管道中，專門處理**中央氣象署 (CWA)** 原始 JSON 數據的 **PySpark** 腳本。

主要腳本 `clean_weather.py` 負責將結構複雜且包含 BigQuery 不相容元素的原始 JSON 數據，轉換成高效的 Parquet 格式，為下游的 BigQuery 載入和 dbt 倉儲建模做準備。

## 📁 核心檔案

| 檔案名稱 | 說明與職責 | 
 | ----- | ----- | 
| **`clean_weather.py`** | **主要 ETL 腳本。** 運行於 Dataproc 叢集上，執行數據的讀取、轉換、清洗和寫入操作。 | 
| `clean_weather.ipynb` | 開發過程中的互動式測試筆記本（非生產環境使用）。 | 

## 💡 技術選型考量：為何使用 PySpark (Dataproc)

雖然目前的氣象數據量相對較小，可以直接使用 Airflow 內的 Python 或 BigQuery 腳本處理，但為了**實戰大型數據架構**並確保專案具備**水平擴展性 (Scalability)**，我們刻意設計了 PySpark 處理步驟：

* **實戰練習：** 提供在真實雲端環境 (Dataproc) 上，運行分散式計算框架 PySpark 的經驗。

* **應對未來增長：** 提前建立起能夠處理 TB 級別數據的技術棧。若未來需要整合更多歷史數據源或更高頻的傳感器數據，此架構**無需重新設計**，即可高效運行。

* **環境隔離：** 利用 Dataproc 的**動態叢集**能力，確保數據清洗的資源與 Airflow 調度器本身完全隔離，提高了整個管道的穩定性。

## 🚀 數據處理流程 (`clean_weather.py`)

該腳本被 Apache Airflow DAG (`integrated_data_pipeline`) 調度，作為 Dataproc 上的 Spark Job 運行。

### 1. 數據來源與目標

* **輸入 (Input):** 從 GCS 讀取 CWA 原始 JSON 檔案 (例如 `gs://{bucket}/weather_raw/{DATE_PATH}/...json`)。

* **輸出 (Output):** 將清洗後的數據寫入 GCS 為 Parquet 檔案，並依日期進行分區 (例如 `gs://{bucket}/weather_cleaned_parquet/{DATE_PATH}/`)。

### 2. ETL 核心邏輯

1. **讀取與展開 (Read & Explode):** 讀取原始 JSON 檔案，並使用 PySpark 的 `explode` 函數，將 JSON 中嵌套的 `Station` 陣列攤平 (Flattening)。

2. **欄位選擇與保留 (Select & Preserve):** 選擇必要的欄位，並**刻意保留** `ObsTime`、`GeoInfo`、`WeatherElement` 等複雜結構為 PySpark `STRUCT` 類型。

3. **結構化優化 (Structured Optimization):** 這種保留嵌套結構的策略，是為了配合 BigQuery 在後續 dbt 轉換步驟中，使用 `UNNEST` 函數進行高效的進一步處理。

4. **寫入 Parquet:** 以 Parquet 格式將結果寫入 GCS，Parquet 相比 JSON 具有更高的壓縮率和查詢效率。

### 3. 運行方式 (Airflow/Dataproc)

該腳本需透過 DataprocSubmitJobOperator 提交到 Google Dataproc 暫時叢集上運行。

#### 執行命令 (範例)

腳本接收一個日期參數 (`YYYY/MM/DD`) 作為輸入：