import asyncio
import json
import time
import sys
import os
import psutil
import aiocoap
import paho.mqtt.client as mqtt

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from utils.logger import log_event

MQTT_BROKER = "127.0.0.1"
MQTT_PORT = 1883
TOPIC_IN = "iot/mqtt_to_coap"
TOPIC_OUT = "iot/coap_to_mqtt"
COAP_URI_FROM_MQTT = "coap://127.0.0.1:5683/sensor/from_mqtt"
COAP_URI_TO_MQTT = "coap://127.0.0.1:5683/sensor/to_mqtt"

mqtt_to_coap_queue = asyncio.Queue()
last_processed_coap_id = None

# 1. Modul Penerima MQTT (Callback)
def on_connect(client, userdata, flags, rc):
    print(f"[Conversion] Terhubung ke MQTT Broker (rc={rc})")
    client.subscribe(TOPIC_IN)

def on_message(client, userdata, msg):
    recv_ts = time.time()
    try:
        raw_payload = msg.payload.decode("utf-8")
        data = json.loads(raw_payload)
        userdata["loop"].call_soon_threadsafe(
            mqtt_to_coap_queue.put_nowait, (data, raw_payload, recv_ts)
        )
    except Exception as e:
        print(f"[Conversion] Payload MQTT tidak valid: {e}")

# 2. Worker Translasi MQTT -> CoAP POST
async def worker_mqtt_to_coap(coap_ctx):
    while True:
        data, raw_payload, recv_ts = await mqtt_to_coap_queue.get()
        msg_id = data.get("id", "unknown")
        seq = data.get("sequence", -1)
        log_event("conversion.csv", msg_id, seq, "MQTT_RECV", timestamp=recv_ts)

        start_conv_ts = time.time()
        request = aiocoap.Message(
            code=aiocoap.POST,
            payload=raw_payload.encode("utf-8"),
            uri=COAP_URI_FROM_MQTT
        )
        try:
            response = await coap_ctx.request(request).response
            end_conv_ts = time.time()
            log_event("conversion.csv", msg_id, seq, "COAP_POST_SENT", timestamp=end_conv_ts, status=str(response.code))
            print(f"[MQTT->CoAP] ID: {msg_id} | Delay konversi: {(end_conv_ts - recv_ts)*1000:.2f} ms")
        except Exception as e:
            log_event("conversion.csv", msg_id, seq, "COAP_POST_FAIL", status="ERR", detail=str(e))

# 3. Worker Polling CoAP GET (Interval 1 detik) -> MQTT Publish
async def worker_coap_to_mqtt(coap_ctx, mqtt_client):
    global last_processed_coap_id
    while True:
        await asyncio.sleep(1.0)  # Interval polling 1 detik sesuai spesifikasi
        poll_ts = time.time()
        request = aiocoap.Message(code=aiocoap.GET, uri=COAP_URI_TO_MQTT)
        try:
            response = await coap_ctx.request(request).response
            if not response.payload:
                continue

            raw_payload = response.payload.decode("utf-8")
            data = json.loads(raw_payload)
            msg_id = data.get("id")
            seq = data.get("sequence", -1)

            # Deduplikasi: skip jika ID sudah pernah diproses
            if msg_id == last_processed_coap_id:
                log_event("conversion.csv", msg_id, seq, "POLL_SKIP_DUP", timestamp=poll_ts)
                continue

            last_processed_coap_id = msg_id
            log_event("conversion.csv", msg_id, seq, "COAP_GET_NEW", timestamp=poll_ts)

            mqtt_client.publish(TOPIC_OUT, raw_payload)
            pub_ts = time.time()
            log_event("conversion.csv", msg_id, seq, "MQTT_PUB_SENT", timestamp=pub_ts)
            print(f"[CoAP->MQTT] ID baru diteruskan: {msg_id}")
        except Exception as e:
            pass

# 4. Monitor Resource CPU & Memori proses Conversion Server
async def monitor_resources():
    proc = psutil.Process(os.getpid())
    proc.cpu_percent(interval=None)
    while True:
        await asyncio.sleep(1.0)
        cpu = proc.cpu_percent(interval=None)
        mem_mb = proc.memory_info().rss / (1024 * 1024)
        log_event("system_metrics.csv", "SYS", 0, "RESOURCE", status=f"CPU:{cpu}%", detail=f"MEM:{mem_mb:.2f}MB")

async def main():
    loop = asyncio.get_running_loop()
    mqtt_client = mqtt.Client(userdata={"loop": loop})
    mqtt_client.on_connect = on_connect
    mqtt_client.on_message = on_message
    mqtt_client.connect(MQTT_BROKER, MQTT_PORT, 60)
    mqtt_client.loop_start()

    coap_ctx = await aiocoap.Context.create_client_context()
    print("[Conversion Server] Aktif (MQTT <-> CoAP)...")

    await asyncio.gather(
        worker_mqtt_to_coap(coap_ctx),
        worker_coap_to_mqtt(coap_ctx, mqtt_client),
        monitor_resources()
    )

if __name__ == "__main__":
    asyncio.run(main())