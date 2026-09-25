import threading
import paho.mqtt.client as mqtt
import psycopg2
import json
import time
import os
from flask import Flask, request, jsonify
from flask_cors import CORS
from datetime import datetime, timezone

# Konfigurasi
MQTT_HOST = os.getenv("MQTT_HOST", "mosquitto")
MQTT_PORT = 1883
DB_HOST = os.getenv("DB_HOST", "postgres")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASS = os.getenv("DB_PASS", "postgres")
DB_NAME = os.getenv("DB_NAME", "lindu_db")

time.sleep(5)
conn = psycopg2.connect(host=DB_HOST, user=DB_USER, password=DB_PASS, dbname=DB_NAME)
conn.autocommit = False
cursor = conn.cursor()

# Buffer antrean untuk Batch Insert (Sangat Efisien)
telemetry_buffer = []
status_buffer = []
node_location_buffer = {}  # node_id -> (lat, lon, updated_at); dict agar hanya simpan lokasi terbaru per node
buffer_lock = threading.Lock()

def db_writer_thread():
    global telemetry_buffer, status_buffer, node_location_buffer
    while True:
        time.sleep(0.5) # Flush ke database setiap 0.5 detik

        with buffer_lock:
            local_telemetry = telemetry_buffer[:]
            local_status = status_buffer[:]
            local_locations = list(node_location_buffer.items())
            telemetry_buffer.clear()
            status_buffer.clear()
            node_location_buffer.clear()

        if local_telemetry:
            try:
                cursor.executemany(
                    "INSERT INTO sensor_telemetry (time, node_id, pga, rms, accel_x, accel_y, accel_z, temperature, pressure, humidity, latency_ms, valve_status, gas_raw, gas_alert, freq_hz, sensor_ts) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                    local_telemetry
                )
                conn.commit()
            except Exception as e:
                print(f"Error Batch Telemetry: {e}")
                conn.rollback()
                
        if local_status:
            try:
                cursor.executemany(
                    "INSERT INTO sensor_status (time, node_id, status, pose, tilt_angle, latency_ms, sensor_ok, fw_version, ota_status, motion_detected) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                    local_status
                )
                conn.commit()
            except Exception as e:
                print(f"Error Batch Status: {e}")
                conn.rollback()

        if local_locations:
            try:
                cursor.executemany(
                    """INSERT INTO tb_nodes (node_id, lat, lon, updated_at) VALUES (%s, %s, %s, %s)
                       ON CONFLICT (node_id) DO UPDATE SET lat = EXCLUDED.lat, lon = EXCLUDED.lon, updated_at = EXCLUDED.updated_at""",
                    [(node_id, lat, lon, ts) for node_id, (lat, lon, ts) in local_locations]
                )
                conn.commit()
            except Exception as e:
                print(f"Error Batch Node Location: {e}")
                conn.rollback()

threading.Thread(target=db_writer_thread, daemon=True).start()


def on_connect(client, userdata, flags, rc):
    print(f"Ingester terhubung (rc={rc})")
    client.subscribe("lindu/sensor/#")

def on_message(client, userdata, msg):
    try:
        topic = msg.topic
        print(f'Received: {topic}')
        payload = json.loads(msg.payload.decode('utf-8'))
        now = datetime.now(timezone.utc)
        
        if topic.endswith("/data") or topic.endswith("/telemetry"):
            node_id = topic.split("/")[2]
            pga = payload.get("pga", 0.0)
            rms = payload.get("sta_lta", payload.get("rms", 0.0))
            ax = payload.get("ax", payload.get("dyn_x", 0.0))
            ay = payload.get("ay", payload.get("dyn_y", 0.0))
            az = payload.get("az", payload.get("dyn_z", 0.0))
            
            # Atmospheric Data (Opsional jika belum dikirim)
            temp = payload.get("temperature", None)
            press = payload.get("pressure", None)
            hum = payload.get("humidity", None)
            valve = payload.get("valve_status", "UNKNOWN")
            gas_raw = payload.get("gas_raw", None)
            gas_alert = payload.get("gas_alert", None)

            # Fitur wajib dataset ML. Sengaja None (bukan 0) kalau node tidak
            # mengirimnya, supaya baris tanpa frekuensi bisa dibuang saat ekspor
            # dataset alih-alih diam-diam dianggap 0 Hz.
            freq_hz = payload.get("freq_hz", None)


            # Calculate Latency (if ESP32 sends its NTP synced epoch timestamp)
            sent_ts = payload.get("ts", payload.get("timestamp", None))
            latency = None
            if sent_ts:
                latency = (time.time() - sent_ts) * 1000.0 # Convert to milliseconds

            with buffer_lock:
                telemetry_buffer.append((now, node_id, pga, rms, ax, ay, az, temp, press, hum, latency, valve, gas_raw, gas_alert, freq_hz, sent_ts))
            
        elif topic.endswith("/status"):
            node_id = payload.get("node_id", "unknown")
            status = payload.get("status", "unknown")
            pose = payload.get("pose", "unknown")
            tilt = payload.get("tilt_angle", 0.0)
            
            
            sent_ts = payload.get("ts", payload.get("timestamp", None))
            latency = None
            if sent_ts:
                latency = (time.time() - sent_ts) * 1000.0
                
            
            sensor_ok = payload.get("sensor_ok", False)
            fw_version = payload.get("fw_version", "UNKNOWN")
            ota_status = payload.get("ota_status", "IDLE")
            motion_detected = payload.get("motion_detected", None)

            lat = payload.get("lat", None)
            lon = payload.get("lon", None)

            with buffer_lock:
                status_buffer.append((now, node_id, status, pose, tilt, latency, sensor_ok, fw_version, ota_status, motion_detected))
                if lat is not None and lon is not None and (lat != 0.0 or lon != 0.0):
                    node_location_buffer[node_id] = (lat, lon, now)
    except Exception as e:
        print(f"Error: {e}")

client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message

while True:
    try:
        client.connect(MQTT_HOST, MQTT_PORT, 60)
        break
    except:
        time.sleep(2)


# Jalankan MQTT di background
client.loop_start()

# Setup Flask API
app = Flask(__name__)
CORS(app)

@app.route('/api/cmd', methods=['POST'])
def send_cmd():
    try:
        data = request.json
        cmd = data.get('cmd')
        target = data.get('target_node', 'all')
        if not cmd:
            return jsonify({"error": "Missing cmd"}), 400
            
        payload = {"cmd": cmd, "target_node": target}
        if cmd == "set_location":
            payload["lat"] = float(data.get("lat", 0.0))
            payload["lon"] = float(data.get("lon", 0.0))
        elif cmd == "set_broker":
            payload["server"] = data.get("server", "192.168.68.105")
        elif cmd == "simulate_quake":
            # Tombol debugging: memicu alarm gempa PENUH (siren fisik di node +
            # banner di dashboard) tanpa perlu 2 node fisik untuk saling
            # konfirmasi. Node simulasi & magnitudo dibuat sintetis.
            epi_lat = float(data.get("lat") or -6.2018)
            epi_lon = float(data.get("lon") or 106.7823)
            magnitude = float(data.get("magnitude") or 6.5)
            payload = {
                "cmd": "trigger_siren",
                "level": "CRITICAL",
                "epicenter_lat": epi_lat,
                "epicenter_lon": epi_lon,
                "radius_km": round(10 ** (0.5 * magnitude - 1.0), 1),
                "confidence": 95,
                "magnitude": magnitude,
                "measured_velocity_kms": 7.2,
                "time_diff_s": 1.1,
                "bypass": True,
                "triggering_nodes": [
                    {"id": "SIMULATED_NODE_1", "lat": epi_lat + 0.01, "lon": epi_lon - 0.01, "pga": 0.55},
                    {"id": "SIMULATED_NODE_2", "lat": epi_lat - 0.01, "lon": epi_lon + 0.01, "pga": 0.48}
                ],
                "desc": f"🧪 SIMULASI Gempa M{magnitude} (dipicu manual dari Grafana untuk keperluan testing/debugging, tanpa 2 node fisik)."
            }
        client.publish("lindu/actuator/cmd/all", json.dumps(payload))
        return jsonify({"status": "success"})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)

