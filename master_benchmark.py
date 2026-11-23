import subprocess
import sys
import os
import time
import shutil
import asyncio
import json
import pandas as pd
import matplotlib.pyplot as plt
import aiocoap
import paho.mqtt.client as mqtt

# ==============================================================================
# KONFIGURASI EKSPERIMEN
# Ubah DURATION_SEC = 600 dan REPETITIONS = 10 untuk eksperimen penuh (R1 & R2).
# Untuk uji coba cepat dulu, set DURATION_SEC = 10 dan REPETITIONS = 1.
# ==============================================================================
DURATION_SEC = 600      # 600 detik = 10 menit sesuai matriks R1 & R2
REPETITIONS = 10        # 10 pengulangan sesuai matriks R1 & R2
RATES = {"R1": 10, "R2": 20}
PAYLOAD_SIZE_KB = 10
DUMMY_PAYLOAD = "X" * (PAYLOAD_SIZE_KB * 1024)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
LOGS_DIR = os.path.join(BASE_DIR, "logs")
RESULTS_DIR = os.path.join(BASE_DIR, "results")

def clear_logs():
    if os.path.exists(LOGS_DIR):
        shutil.rmtree(LOGS_DIR)
    os.makedirs(LOGS_DIR, exist_ok=True)

def start_background_processes():
    procs = []
    scripts = [
        os.path.join(BASE_DIR, "server", "coap_server.py"),
        os.path.join(BASE_DIR, "server", "conversion_server.py"),
        os.path.join(BASE_DIR, "client", "coap_clients.py"),
        os.path.join(BASE_DIR, "client", "mqtt_clients.py"),
    ]
    for s in scripts:
        p = subprocess.Popen([sys.executable, s], cwd=BASE_DIR)
        procs.append(p)
        time.sleep(0.5)
    time.sleep(2.0)  # Beri waktu koneksi MQTT & CoAP siap
    return procs

def stop_background_processes(procs):
    for p in procs:
        p.terminate()
    for p in procs:
        try:
            p.wait(timeout=3)
        except Exception:
            p.kill()

def send_mqtt_traffic(rate_per_sec, duration_sec):
    sent_csv = os.path.join(LOGS_DIR, "mqtt_pub_sent.csv")
    client = mqtt.Client()
    client.connect("127.0.0.1", 1883, 60)
    client.loop_start()

    total_messages = rate_per_sec * duration_sec
    interval = 1.0 / rate_per_sec
    start_time = time.perf_counter()

    with open(sent_csv, "w", encoding="utf-8") as f:
        f.write("msg_id,sequence,sent_ts\n")
        for seq in range(1, total_messages + 1):
            msg_id = f"mqtt-{seq:06d}"
            sent_ts = time.time()
            payload = {
                "id": msg_id,
                "sequence": seq,
                "sent_ts": sent_ts,
                "source": "mqtt_pub",
                "payload": DUMMY_PAYLOAD
            }
            client.publish("iot/mqtt_to_coap", json.dumps(payload))
            f.write(f"{msg_id},{seq},{sent_ts}\n")

            target_time = start_time + (seq * interval)
            sleep_time = target_time - time.perf_counter()
            if sleep_time > 0:
                time.sleep(sleep_time)

    time.sleep(2.0)  # Tunggu sisa paket dalam antrean selesai diproses
    client.loop_stop()
    client.disconnect()

async def send_coap_traffic(rate_per_sec, duration_sec):
    sent_csv = os.path.join(LOGS_DIR, "coap_post_sent.csv")
    context = await aiocoap.Context.create_client_context()

    total_messages = rate_per_sec * duration_sec
    interval = 1.0 / rate_per_sec
    start_time = time.perf_counter()

    with open(sent_csv, "w", encoding="utf-8") as f:
        f.write("msg_id,sequence,sent_ts\n")
        for seq in range(1, total_messages + 1):
            msg_id = f"coap-{seq:06d}"
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
                payload=json.dumps(payload).encode("utf-8"),
                uri="coap://127.0.0.1:5683/sensor/to_mqtt"
            )
            try:
                await context.request(request).response
                f.write(f"{msg_id},{seq},{sent_ts}\n")
            except Exception:
                pass

            target_time = start_time + (seq * interval)
            sleep_time = target_time - time.perf_counter()
            if sleep_time > 0:
                await asyncio.sleep(sleep_time)

    await asyncio.sleep(2.0)  # Tunggu siklus polling GET terakhir

def run_all_experiments():
    os.makedirs(RESULTS_DIR, exist_ok=True)
    for label, rate in RATES.items():
        for rep in range(1, REPETITIONS + 1):
            print(f"\n==========================================")
            print(f"MENJALANKAN {label} ({rate} msg/s) - PENGULANGAN {rep}/{REPETITIONS}")
            print(f"==========================================")

            # 1. Uji Arah MQTT -> CoAP
            clear_logs()
            procs = start_background_processes()
            try:
                print(f"[{label} Rep {rep}] Fase 1: MQTT -> CoAP ({DURATION_SEC}s)...")
                send_mqtt_traffic(rate, DURATION_SEC)
            finally:
                stop_background_processes(procs)

            dest_mqtt = os.path.join(RESULTS_DIR, f"{label}_MQTT_to_CoAP_rep{rep}")
            shutil.copytree(LOGS_DIR, dest_mqtt, dirs_exist_ok=True)

            # 2. Uji Arah CoAP -> MQTT
            clear_logs()
            procs = start_background_processes()
            try:
                print(f"[{label} Rep {rep}] Fase 2: CoAP -> MQTT ({DURATION_SEC}s)...")
                asyncio.run(send_coap_traffic(rate, DURATION_SEC))
            finally:
                stop_background_processes(procs)

            dest_coap = os.path.join(RESULTS_DIR, f"{label}_CoAP_to_MQTT_rep{rep}")
            shutil.copytree(LOGS_DIR, dest_coap, dirs_exist_ok=True)

def generate_tables_and_charts():
    print("\nMengolah seluruh hasil CSV menjadi Tabel dan Grafik...")
    summary_rows = []
    e2e_records = []
    conv_records = []
    cpu_records = []

    for label, rate in RATES.items():
        for direction in ["MQTT_to_CoAP", "CoAP_to_MQTT"]:
            for rep in range(1, REPETITIONS + 1):
                folder = os.path.join(RESULTS_DIR, f"{label}_{direction}_rep{rep}")
                if not os.path.exists(folder):
                    continue

                if direction == "MQTT_to_CoAP":
                    sent_path = os.path.join(folder, "mqtt_pub_sent.csv")
                    # Gunakan coap_server.csv (event POST_FROM_MQTT) atau coap_get_recv.csv
                    recv_path = os.path.join(folder, "coap_get_recv.csv")
                else:
                    sent_path = os.path.join(folder, "coap_post_sent.csv")
                    recv_path = os.path.join(folder, "mqtt_sub_recv.csv")

                if os.path.exists(sent_path) and os.path.exists(recv_path):
                    df_s = pd.read_csv(sent_path)
                    df_r = pd.read_csv(recv_path)
                    n_sent = df_s["msg_id"].nunique()
                    n_recv = df_r["msg_id"].nunique()
                    succ = (n_recv / n_sent * 100) if n_sent > 0 else 0
                    loss = 100.0 - succ
                    summary_rows.append({
                        "Skenario": label, "Arah": direction, "Rep": rep,
                        "Dikirim": n_sent, "Diterima": n_recv,
                        "Keberhasilan_Persen": succ, "Conversion_Loss_Persen": loss
                    })

                    if not df_r.empty and "sent_ts" in df_r.columns:
                        t0 = df_r["sent_ts"].min()
                        df_r["minute"] = ((df_r["sent_ts"] - t0) // 60).astype(int) + 1
                        df_r["Skenario"] = label
                        df_r["Arah"] = direction
                        e2e_records.append(df_r[["Skenario", "Arah", "minute", "e2e_delay"]])

                # Olah Conversion Delay dari conversion.csv
                conv_path = os.path.join(folder, "conversion.csv")
                if os.path.exists(conv_path):
                    df_c = pd.read_csv(conv_path)
                    if direction == "MQTT_to_CoAP":
                        t_in = df_c[df_c["event"] == "MQTT_RECV"][["message_id", "timestamp"]].rename(columns={"timestamp": "t_in"})
                        t_out = df_c[df_c["event"] == "COAP_POST_SENT"][["message_id", "timestamp"]].rename(columns={"timestamp": "t_out"})
                    else:
                        t_in = df_c[df_c["event"] == "COAP_GET_NEW"][["message_id", "timestamp"]].rename(columns={"timestamp": "t_in"})
                        t_out = df_c[df_c["event"] == "MQTT_PUB_SENT"][["message_id", "timestamp"]].rename(columns={"timestamp": "t_out"})

                    merged = pd.merge(t_in, t_out, on="message_id")
                    if not merged.empty:
                        merged["conv_delay"] = merged["t_out"] - merged["t_in"]
                        t0 = merged["t_in"].min()
                        merged["minute"] = ((merged["t_in"] - t0) // 60).astype(int) + 1
                        merged["Skenario"] = label
                        merged["Arah"] = direction
                        conv_records.append(merged[["Skenario", "Arah", "minute", "conv_delay"]])

                # Olah CPU & Memori dari system_metrics.csv
                sys_path = os.path.join(folder, "system_metrics.csv")
                if os.path.exists(sys_path):
                    df_sys = pd.read_csv(sys_path)
                    if not df_sys.empty:
                        df_sys["cpu"] = df_sys["status"].str.replace("CPU:", "").str.replace("%", "").astype(float)
                        df_sys["mem_mb"] = df_sys["detail"].str.replace("MEM:", "").str.replace("MB", "").astype(float)
                        t0 = df_sys["timestamp"].min()
                        df_sys["minute"] = ((df_sys["timestamp"] - t0) // 60).astype(int) + 1
                        df_sys["Skenario"] = label
                        df_sys["Arah"] = direction
                        cpu_records.append(df_sys[["Skenario", "Arah", "minute", "cpu", "mem_mb"]])

    # Simpan Tabel 1 & 2 (CSV Ringkasan)
    if summary_rows:
        df_sum = pd.DataFrame(summary_rows)
        df_sum.to_csv(os.path.join(RESULTS_DIR, "tabel_ringkasan_per_pengulangan.csv"), index=False)
        tabel_utama = df_sum.groupby(["Skenario", "Arah"])[["Keberhasilan_Persen", "Conversion_Loss_Persen"]].mean()
        tabel_utama.to_csv(os.path.join(RESULTS_DIR, "TABEL_1_DAN_2_KEBERHASILAN_DAN_LOSS.csv"))
        print("\n=== TABEL 1 & 2: RERATA KEBERHASILAN & CONVERSION LOSS (%) ===")
        print(tabel_utama)

    # Simpan Ringkasan Puncak & Persentil 95 CPU/Memori
    if cpu_records:
        df_cpu_all = pd.concat(cpu_records)
        stats_cpu = df_cpu_all.groupby(["Skenario", "Arah"]).agg(
            CPU_Mean=("cpu", "mean"),
            CPU_P95=("cpu", lambda x: x.quantile(0.95)),
            CPU_Peak=("cpu", "max"),
            MEM_Mean_MB=("mem_mb", "mean"),
            MEM_P95_MB=("mem_mb", lambda x: x.quantile(0.95)),
            MEM_Peak_MB=("mem_mb", "max")
        )
        stats_cpu.to_csv(os.path.join(RESULTS_DIR, "STATISTIK_CPU_DAN_MEMORI.csv"))
        print("\n=== STATISTIK CPU & MEMORI CONVERSION SERVER ===")
        print(stats_cpu)

    # Plot 3 Grafik Wajib
    def save_grouped_plot(records, val_col, title, ylabel, filename):
        if not records:
            return
        df_all = pd.concat(records)
        df_all = df_all[df_all["minute"] <= 10]
        plt.figure(figsize=(9, 5))
        for (sken, arah), grp in df_all.groupby(["Skenario", "Arah"]):
            per_min = grp.groupby("minute")[val_col].mean()
            plt.plot(per_min.index, per_min.values, marker="o", label=f"{sken} - {arah}")
        plt.title(title, fontweight="bold")
        plt.xlabel("Waktu Pengujian (Menit ke-)")
        plt.ylabel(ylabel)
        plt.xticks(range(1, 11))
        plt.grid(True, linestyle="--", alpha=0.6)
        plt.legend()
        plt.tight_layout()
        out_path = os.path.join(RESULTS_DIR, filename)
        plt.savefig(out_path, dpi=300)
        plt.close()
        print(f"[Grafik Tersimpan] {out_path}")

    save_grouped_plot(e2e_records, "e2e_delay", "Grafik 1: Rerata End-to-End Delay per Menit (R1 & R2)", "Delay (Detik)", "GRAFIK_1_E2E_DELAY.png")
    save_grouped_plot(conv_records, "conv_delay", "Grafik 2: Rerata Conversion Delay per Menit (R1 & R2)", "Delay (Detik)", "GRAFIK_2_CONVERSION_DELAY.png")
    save_grouped_plot(cpu_records, "cpu", "Grafik 3: Rerata Penggunaan CPU Conversion Server per Menit (R1 & R2)", "CPU Usage (%)", "GRAFIK_3_CPU_USAGE.png")

if __name__ == "__main__":
    run_all_experiments()
    generate_tables_and_charts()
    print("\nSELESAI! Semua CSV, Tabel, dan Grafik tersimpan di folder 'results/'.")