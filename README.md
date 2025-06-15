
# Real-time IoT Data Pipeline with Kafka & PySpark

## 🚀 Overview

This project implements a real-time data ingestion and processing pipeline using **Apache Kafka**, **PySpark Structured Streaming**, and **Pandas**. The goal is to simulate IoT sensor data streaming into Kafka, process it with Spark, and periodically store and consolidate the output into CSV format.

---

## 📦 Project Components

### 1. **Kafka Producer**

* Sends JSON-formatted sensor data (sensor\_id, temperature, humidity, timestamp) to Kafka topic `sensor-data`.
* Simulates real-time data flow.

### 2. **PySpark Consumer**

* Subscribes to `sensor-data` Kafka topic.
* Parses and validates the incoming JSON data using a predefined schema.
* Writes data into CSV files in a target output directory every few seconds.

### 3. **Automated File Handling**

* Runs the Spark consumer for a fixed duration (e.g., 2 minutes).
* After completion:

  * Merges all CSV part files into a single clean CSV file with correct headers.
  * Moves the merged file into a `backup/` directory with a timestamped filename.
  * Cleans up the output and checkpoint directories.
  * The whole cycle can be repeated to collect fresh data.

---

## 🗂️ Project Structure

```
project/
├── producer.ipynb           # Kafka producer script (sends sensor data)
├── consumer.ipynb           # PySpark consumer with batch-interval control
├── merger.py / .ipynb       # Pandas-based file merge and cleanup logic
├── D:/sensor-data/
│   ├── output/              # Spark CSV output files (temporary)
│   ├── backup/              # Merged and archived CSV files
├── D:/hadoop/checkpoint/    # Spark streaming checkpointing
```

---

## ⚙️ How It Works

1. **Run Kafka and Zookeeper** locally.
2. **Start the Kafka producer** to send streaming sensor data.
3. **Run the PySpark consumer** (for a fixed time, e.g., 120s).
4. **Automatically merge part-\*.csv** into a single file with clean headers.
5. **Move merged file to backup and delete old output/checkpoint**.

---

## 💻 Tech Stack

* **Apache Kafka** – Real-time message streaming
* **Apache Spark** (Structured Streaming) – Real-time data processing
* **Pandas** – Data post-processing and file merging
* **Python** (Jupyter Notebook) – Orchestration and scripting
* **Hadoop Winutils** – Required for Spark on Windows

---

## 📄 Sample Output

```
sensor_id,temperature,humidity,timestamp
1,29.5,66.2,2025-06-15 09:12:01
2,31.2,63.1,2025-06-15 09:12:02
...
```

---

## 🔁 Next Improvements (Optional)

* Add automatic loop to restart pipeline continuously.
* Store data in Parquet format for efficiency.
* Integrate with a monitoring dashboard (e.g., Streamlit).
* Add unit tests and logging.

---

## 📌 Requirements

* Python 3.11+
* PySpark 3.3.2
* Apache Kafka
* Hadoop 3.3.2 (Winutils)
* pandas, numpy

---

## ✍️ Author

*This project is developed as part of a data engineering portfolio.*

Feel free to clone, modify and reuse for learning or interviews!
