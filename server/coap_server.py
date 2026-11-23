import asyncio
import json
import sys
import os
import aiocoap
import aiocoap.resource as resource

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from utils.logger import log_event

class SensorResource(resource.Resource):
    def __init__(self, name):
        super().__init__()
        self.name = name
        self.latest_payload = b""

    async def render_post(self, request):
        self.latest_payload = request.payload
        try:
            data = json.loads(self.latest_payload.decode("utf-8"))
            msg_id = data.get("id", "unknown")
            seq = data.get("sequence", -1)
            log_event("coap_server.csv", msg_id, seq, f"POST_{self.name}", status="2.04_CHANGED")
            print(f"[CoAP Server] POST diterima di /{self.name} | ID: {msg_id}")
        except Exception as e:
            print(f"[CoAP Server] Error parsing POST: {e}")
        return aiocoap.Message(code=aiocoap.CHANGED, payload=self.latest_payload)

    async def render_get(self, request):
        return aiocoap.Message(code=aiocoap.CONTENT, payload=self.latest_payload)

async def main():
    root = resource.Site()
    root.add_resource(["sensor", "from_mqtt"], SensorResource("from_mqtt"))
    root.add_resource(["sensor", "to_mqtt"], SensorResource("to_mqtt"))

    await aiocoap.Context.create_server_context(root, bind=("127.0.0.1", 5683))
    print("[CoAP Server] Berjalan di 127.0.0.1:5683 (UDP)...")
    await asyncio.get_running_loop().create_future()

if __name__ == "__main__":
    asyncio.run(main())