# mqtt_subscriber.py (Jalankan di terminal terpisah saat pengujian)
import paho.mqtt.client as mqtt
import json
import time
from logger_utils import init_csv, log_event

log_file = init_csv("mqtt_sub_recv.csv", ["msg_id", "sent_ts", "recv_ts", "e2e_delay", "status"])

def on_message(client, userdata, msg):
    recv_ts = time.time()
    try:
        data = json.loads(msg.payload.decode())
        msg_id = data.get("id")
        sent_ts = float(data.get("sent_ts", 0))
        e2e_delay = recv_ts - sent_ts
        
        print(f"[MQTT Sub] Diterima ID: {msg_id} | E2E Delay: {e2e_delay:.4f}s")
        log_event(log_file, [msg_id, sent_ts, recv_ts, e2e_delay, "VALID"])
    except Exception as e:
        print(f"[MQTT Sub] Error validasi: {e}")

client = mqtt.Client()
client.on_message = on_message
client.connect("127.0.0.1", 1883, 60)
client.subscribe("iot/coap_to_mqtt")

print("[MQTT Subscriber] Menunggu pesan dari topik iot/coap_to_mqtt...")
client.loop_forever()