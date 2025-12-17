# 👁️ The Omniscient Video Brain (Real-Time AI Pipeline)
<img width="1440" height="900" alt="Screenshot 2025-12-16 at 7 23 46 PM" src="https://github.com/user-attachments/assets/962bb64f-d95a-457d-91d1-0bcce297654b" />


**A Real-Time Video Analytics Pipeline processing live streams using Apache Kafka (Redpanda), YOLOv8, and DuckDB.**

## 🚀 The Architecture
This system treats video as a data source. It ingests a live YouTube traffic cam, breaks it into frames, processes them with Computer Vision to detect objects (Cars, Pedestrians), and enables SQL queries on the video data in real-time.

**Tech Stack:**
* **Ingestion:** Python + OpenCV (Live Stream Capture)
* **Streaming:** Redpanda (Kafka-compatible Event Streaming)
* **AI/ML:** YOLOv8 (Object Detection & Inference)
* **Storage:** DuckDB (OLAP Database for Analytics)
* **Infrastructure:** Docker (Containerization)

## 🛠️ How It Works
1.  **The Eyes (`ingest.py`):** Connects to a live YouTube RTSP stream, captures frames at 1 FPS, serializes them, and produces them to a Kafka Topic.
2.  **The Brain (`process.py`):** Consumes the Kafka stream, decodes the images, runs the YOLOv8 Neural Network to detect objects, and writes metadata to DuckDB.
3.  **The Memory (`analysis`):** Allows SQL queries on the live data (e.g., "Count how many trucks passed in the last 5 minutes").

## 💻 How to Run
1.  **Start Infrastructure:**
    ```bash
    docker run -d --name redpanda-1 --rm -p 9092:9092 -p 9644:9644 docker.io/redpandadata/redpanda:latest redpanda start --mode dev-container
    ```
2.  **Start Ingestion:**
    ```bash
    python ingest.py
    ```
3.  **Start Processing:**
    ```bash
    python process.py
    ```

## 📊 Sample Output
```sql
SELECT object_type, count FROM detections ORDER BY time DESC LIMIT 5;

object_type,count
car,8039
person,2158
bus,31
