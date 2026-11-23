import os
import pandas as pd
import matplotlib.pyplot as plt

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
RESULTS_DIR = os.path.join(BASE_DIR, "results")
RATES = {"R1": 10, "R2": 20}
REPETITIONS = 1

def load_csv_smart(path, expected_cols):
    df = pd.read_csv(path)
    # Jika header pertama ternyata data (bukan nama kolom), baca ulang dengan names=expected_cols
    if expected_cols[-1] not in df.columns and len(df.columns) >= len(expected_cols):
        df = pd.read_csv(path, header=None)
        df.columns = expected_cols[:len(df.columns)] + [f"extra_{i}" for i in range(len(df.columns) - len(expected_cols))]
    return df

def generate_all():
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

                # Baca conversion.csv secara fleksibel
                conv_path = os.path.join(folder, "conversion.csv")
                if os.path.exists(conv_path):
                    df_c = load_csv_smart(conv_path, ["timestamp", "message_id", "sequence", "event", "status", "detail"])
                    # Deteksi otomatis kolom event dan message_id
                    event_col = next((c for c in df_c.columns if df_c[c].astype(str).str.contains("MQTT_RECV|COAP_GET_NEW").any()), None)
                    id_col = next((c for c in df_c.columns if df_c[c].astype(str).str.contains("mqtt-|coap-").any()), None)
                    ts_col = next((c for c in df_c.columns if pd.api.types.is_numeric_dtype(df_c[c]) and df_c[c].min() > 1e9), None)

                    if event_col and id_col and ts_col:
                        if direction == "MQTT_to_CoAP":
                            t_in = df_c[df_c[event_col] == "MQTT_RECV"][[id_col, ts_col]].rename(columns={ts_col: "t_in"})
                            t_out = df_c[df_c[event_col] == "COAP_POST_SENT"][[id_col, ts_col]].rename(columns={ts_col: "t_out"})
                        else:
                            t_in = df_c[df_c[event_col] == "COAP_GET_NEW"][[id_col, ts_col]].rename(columns={ts_col: "t_in"})
                            t_out = df_c[df_c[event_col] == "MQTT_PUB_SENT"][[id_col, ts_col]].rename(columns={ts_col: "t_out"})

                        merged = pd.merge(t_in, t_out, on=id_col)
                        if not merged.empty:
                            merged["conv_delay"] = merged["t_out"] - merged["t_in"]
                            t0 = merged["t_in"].min()
                            merged["minute"] = ((merged["t_in"] - t0) // 60).astype(int) + 1
                            merged["Skenario"] = label
                            merged["Arah"] = direction
                            conv_records.append(merged[["Skenario", "Arah", "minute", "conv_delay"]])

                # Baca system_metrics.csv secara fleksibel
                sys_path = os.path.join(folder, "system_metrics.csv")
                if os.path.exists(sys_path):
                    df_sys = load_csv_smart(sys_path, ["timestamp", "message_id", "sequence", "event", "status", "detail"])
                    cpu_col = next((c for c in df_sys.columns if df_sys[c].astype(str).str.contains("CPU:").any()), None)
                    mem_col = next((c for c in df_sys.columns if df_sys[c].astype(str).str.contains("MEM:").any()), None)
                    ts_col = next((c for c in df_sys.columns if pd.api.types.is_numeric_dtype(df_sys[c]) and df_sys[c].min() > 1e9), None)

                    if cpu_col and mem_col and ts_col:
                        df_sys["cpu"] = df_sys[cpu_col].astype(str).str.replace("CPU:", "").str.replace("%", "").astype(float)
                        df_sys["mem_mb"] = df_sys[mem_col].astype(str).str.replace("MEM:", "").str.replace("MB", "").astype(float)
                        t0 = df_sys[ts_col].min()
                        df_sys["minute"] = ((df_sys[ts_col] - t0) // 60).astype(int) + 1
                        df_sys["Skenario"] = label
                        df_sys["Arah"] = direction
                        cpu_records.append(df_sys[["Skenario", "Arah", "minute", "cpu", "mem_mb"]])

    if summary_rows:
        df_sum = pd.DataFrame(summary_rows)
        df_sum.to_csv(os.path.join(RESULTS_DIR, "tabel_ringkasan_detail.csv"), index=False)
        tabel_utama = df_sum.groupby(["Skenario", "Arah"])[["Dikirim", "Diterima", "Keberhasilan_Persen", "Conversion_Loss_Persen"]].mean()
        tabel_utama.to_csv(os.path.join(RESULTS_DIR, "TABEL_1_DAN_2_KEBERHASILAN_DAN_LOSS.csv"))
        print("\n=== TABEL 1 & 2: RERATA KEBERHASILAN & CONVERSION LOSS (%) ===")
        print(tabel_utama)

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

    def save_grouped_plot(records, val_col, title, ylabel, filename):
        if not records:
            print(f"[Info] Data kosong untuk {filename}")
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
    generate_all()