# run_experiment.py
import asyncio
import aiocoap
import paho.mqtt.client as mqtt
import json
import time
from logger_utils import init_csv, log_event

# Konfigurasi ukuran payload (sesuaikan & catat di laporan)[cite: 3]
PAYLOAD_SIZE_KB = 10 
DUMMY_PAYLOAD = "X" * (PAYLOAD_SIZE_KB * 1024)

def run_mqtt_sender(rate_per_sec, duration_sec):
    log_file = init_csv("mqtt_pub_sent.csv", ["msg_id", "sequence", "sent_ts"])
    client = mqtt.Client()
    client.connect("172.16.48.114", 1883, 60)
    
    total_messages = rate_per_sec * duration_sec
    interval = 1.0 / rate_per_sec
    print(f"[Uji MQTT->CoAP] Mengirim {total_messages} pesan ({rate_per_sec} msg/s)...")
    
    for seq in range(1, total_messages + 1):
        start_loop = time.time()
        msg_id = f"mqtt-{seq:05d}"
        sent_ts = time.time()
        
        payload = {
            "id": msg_id,
            "sequence": seq,
            "sent_ts": sent_ts,
            "source": "mqtt_pub",
            "payload": DUMMY_PAYLOAD
        }
        client.publish("iot/mqtt_to_coap", json.dumps(payload))
        log_event(log_file, [msg_id, seq, sent_ts])
        
        elapsed = time.time() - start_loop
        if elapsed < interval:
            time.sleep(interval - elapsed)
            
    client.disconnect()
    print("[Uji MQTT->CoAP] Selesai.")

async def run_coap_sender(rate_per_sec, duration_sec):
    log_file = init_csv("coap_post_sent.csv", ["msg_id", "sequence", "sent_ts"])
    context = await aiocoap.Context.create_client_context()
    
    total_messages = rate_per_sec * duration_sec
    interval = 1.0 / rate_per_sec
    print(f"[Uji CoAP->MQTT] Mengirim {total_messages} pesan ({rate_per_sec} msg/s)...")
    
    for seq in range(1, total_messages + 1):
        start_loop = time.time()
        msg_id = f"coap-{seq:05d}"
        sent_ts = time.time()
        
        payload = {
            "id": msg_id,
            "sequence": seq,
            "sent_ts": sent_ts,
            "source": "coap_post",
            "payload": DUMMY_PAYLOAD
        }
        request = aiocoap.Message(
            code=aiocoap.POST, 
            payload=json.dumps(payload).encode(), 
            uri='coap://172.16.48.114:5683/sensor/to_mqtt'
        )
        try:
            await context.request(request).response
            log_event(log_file, [msg_id, seq, sent_ts])
        except Exception as e:
            print(f"Gagal POST {msg_id}: {e}")
            
        elapsed = time.time() - start_loop
        if elapsed < interval:
            await asyncio.sleep(interval - elapsed)
            
    print("[Uji CoAP->MQTT] Selesai.")

if __name__ == "__main__":
    # Contoh uji coba cepat 5 detik dengan laju 10 pesan/detik[cite: 1]
    run_mqtt_sender(rate_per_sec=10, duration_sec=5)
    # asyncio.run(run_coap_sender(rate_per_sec=10, duration_sec=5))