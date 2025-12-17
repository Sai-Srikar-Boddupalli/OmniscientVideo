import json
import base64
import cv2
import numpy as np
import duckdb
import time
from kafka import KafkaConsumer
from ultralytics import YOLO

# CONFIGURATION
TOPIC_NAME = "live_video_frames"
KAFKA_SERVER = "localhost:9092"
DB_NAME = "traffic_data.duckdb"

def init_db():
    """Creates the database table if it doesn't exist."""
    con = duckdb.connect(DB_NAME)
    con.execute("""
        CREATE TABLE IF NOT EXISTS detections (
            time TIMESTAMP,
            frame_id INTEGER,
            object_type TEXT,
            confidence FLOAT,
            count INTEGER
        )
    """)
    con.close()
    print(f"📁 Database '{DB_NAME}' ready!")

def save_detection(frame_id, objects_found):
    """Saves what we saw into DuckDB."""
    if not objects_found:
        return
        
    con = duckdb.connect(DB_NAME)
    current_time = time.strftime('%Y-%m-%d %H:%M:%S')
    
    # objects_found is a dictionary like {'car': 3, 'person': 1}
    for obj, count in objects_found.items():
        # Insert a row for each object type detected
        query = f"INSERT INTO detections VALUES ('{current_time}', {frame_id}, '{obj}', 0.0, {count})"
        con.execute(query)
    
    con.close()

def main():
    # 1. Setup Database
    init_db()

    print("🧠 Loading AI Model...")
    model = YOLO("yolov8n.pt") 
    print("✅ Model Loaded!")

    print("🎧 Connecting to Redpanda Stream...")
    consumer = KafkaConsumer(
        TOPIC_NAME,
        bootstrap_servers=KAFKA_SERVER,
        auto_offset_reset='latest',
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        api_version=(2, 5, 0)
    )
    print("✅ Connected! Watching and Recording...")

    for message in consumer:
        data = message.value
        frame_id = data['frame_id']
        
        # Decode image
        jpg_original = base64.b64decode(data['image'])
        np_arr = np.frombuffer(jpg_original, dtype=np.uint8)
        frame = cv2.imdecode(np_arr, cv2.IMREAD_COLOR)

        # Run AI
        results = model(frame, verbose=False)
        
        # Count objects (e.g., {'car': 4, 'truck': 1})
        objects_detected = {}
        
        for r in results:
            for box in r.boxes:
                # Get the class name (e.g., 'car')
                class_id = int(box.cls[0])
                name = model.names[class_id]
                
                if name in objects_detected:
                    objects_detected[name] += 1
                else:
                    objects_detected[name] = 1

        # SAVE TO DB
        save_detection(frame_id, objects_detected)

        # Visualization
        annotated_frame = results[0].plot()
        
        # Add a text overlay showing what we are saving
        text = str(objects_detected)
        cv2.putText(annotated_frame, text, (10, 30), cv2.FONT_HERSHEY_SIMPLEX, 1, (0, 255, 0), 2)
        
        cv2.imshow("Omniscient Video Brain", annotated_frame)
        
        if cv2.waitKey(1) & 0xFF == ord('q'):
            break

    cv2.destroyAllWindows()

if __name__ == "__main__":
    main()