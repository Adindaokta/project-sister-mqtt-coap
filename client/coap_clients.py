# coap_get_verifier.py (Untuk memverifikasi alur MQTT -> CoAP)
import asyncio
import aiocoap
import json
import time
from logger_utils import init_csv, log_event

log_file = init_csv("coap_get_recv.csv", ["msg_id", "sent_ts", "recv_ts", "e2e_delay", "status"])

async def verify_coap_resource():
    context = await aiocoap.Context.create_client_context()
    last_verified_id = None
    print("[CoAP GET Client] Memantau resource /sensor/from_mqtt...")
    
    while True:
        request = aiocoap.Message(code=aiocoap.GET, uri='coap://172.16.48.114:5683/sensor/to_mqtt')
        try:
            response = await context.request(request).response
            recv_ts = time.time()
            if response.payload:
                data = json.loads(response.payload.decode())
                msg_id = data.get("id")
                
                if msg_id != last_verified_id:
                    last_verified_id = msg_id
                    sent_ts = float(data.get("sent_ts", 0))
                    e2e_delay = recv_ts - sent_ts
                    print(f"[CoAP GET] Terverifikasi ID: {msg_id} | E2E Delay: {e2e_delay:.4f}s")
                    log_event(log_file, [msg_id, sent_ts, recv_ts, e2e_delay, "VALID"])
        except Exception:
            pass
        await asyncio.sleep(0.05) # Polling cepat untuk verifikasi

if __name__ == "__main__":
    asyncio.run(verify_coap_resource())