import cv2
import time
import json
import base64
import ssl
from kafka import KafkaProducer
from yt_dlp import YoutubeDL

# 1. SSL FIX for Mac
ssl._create_default_https_context = ssl._create_unverified_context

# 2. CONFIGURATION
YOUTUBE_URL = "https://www.youtube.com/watch?v=1EiC9bvVGnk" 
TOPIC_NAME = "live_video_frames"
KAFKA_SERVER = "localhost:9092"

def get_video_stream_url(link):
    options = {
        'format': 'best', 
        'quiet': True,
        'nocheckcertificate': True, 
        'ignoreerrors': True,
        'no_warnings': True
    }
    with YoutubeDL(options) as ydl:
        info = ydl.extract_info(link, download=False)
        return info['url']

def main():
    print("🚀 Connecting to Redpanda...")
    try:
        # 3. CONNECTION FIX: Added api_version and increased timeouts
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_SERVER,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            api_version=(2, 5, 0),       # HELPS PYTHON TALK TO REDPANDA
            request_timeout_ms=10000,    # GIVE IT MORE TIME
            retry_backoff_ms=500
        )
        print("✅ Connected to Redpanda!")
    except Exception as e:
        print(f"❌ Connection Failed: {e}")
        return

    print("🎥 Getting stream URL...")
    try:
        stream_url = get_video_stream_url(YOUTUBE_URL)
    except Exception as e:
        print(f"❌ URL Error: {e}")
        return

    print(f"✅ URL Found! Starting Stream...")
    cap = cv2.VideoCapture(stream_url)
    
    frame_count = 0
    
    while True:
        success, frame = cap.read()
        if not success:
            print("Stream ended or buffering...")
            break

        if frame_count % 30 == 0:
            resized = cv2.resize(frame, (640, 360))
            _, buffer = cv2.imencode('.jpg', resized)
            jpg_as_text = base64.b64encode(buffer).decode('utf-8')

            data = {
                "frame_id": frame_count,
                "timestamp": time.time(),
                "image": jpg_as_text
            }

            # Send data
            producer.send(TOPIC_NAME, data)
            print(f"📤 Sent Frame {frame_count}")

        frame_count += 1
        
        if cv2.waitKey(1) & 0xFF == ord('q'):
            break
            
        time.sleep(0.01)

    cap.release()
    producer.close()

if __name__ == "__main__":
    main()