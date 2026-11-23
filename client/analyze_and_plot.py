# analyze_and_plot.py
import pandas as pd
import matplotlib.pyplot as plt
import os

def calculate_success_and_loss(sent_csv, recv_csv, label):
    if not os.path.exists(sent_csv) or not os.path.exists(recv_csv):
        print(f"[{label}] File CSV belum lengkap.")
        return

    df_sent = pd.read_csv(sent_csv)
    df_recv = pd.read_csv(recv_csv)
    
    n_sent = len(df_sent['msg_id'].unique())
    n_recv = len(df_recv['msg_id'].unique())
    
    # Rumus sesuai Bagian 4.2 panduan
    success_rate = (n_recv / n_sent) * 100 if n_sent > 0 else 0
    loss_rate = ((n_sent - n_recv) / n_sent) * 100 if n_sent > 0 else 0
    
    print(f"\n=== HASIL METRIK: {label} ===")
    print(f"Total Dikirim (N_dikirim) : {n_sent}")
    print(f"Total Diterima (N_tujuan) : {n_recv}")
    print(f"Keberhasilan Konversi     : {success_rate:.2f}%")
    print(f"Conversion Loss           : {loss_rate:.2f}%")

def plot_per_minute(df, time_col, val_col, title, ylabel, output_filename):
    if df.empty:
        return
    # Kelompokkan data per menit (menit 1 s.d. 10)[cite: 5]
    start_ts = df[time_col].min()
    df['minute'] = ((df[time_col] - start_ts) // 60).astype(int) + 1
    df_10min = df[df['minute'] <= 10]
    
    grouped = df_10min.groupby('minute')[val_col].mean()
    
    plt.figure(figsize=(8, 4.5))
    plt.plot(grouped.index, grouped.values, marker='o', linewidth=2, color='#1f77b4')
    plt.title(title, fontsize=12, fontweight='bold')
    plt.xlabel('Waktu Pengujian (Menit ke-)', fontsize=10)
    plt.ylabel(ylabel, fontsize=10)
    plt.xticks(range(1, 11))
    plt.grid(True, linestyle='--', alpha=0.6)
    plt.tight_layout()
    plt.savefig(output_filename, dpi=300)
    plt.close()
    print(f"[Grafik] Tersimpan: {output_filename}")

if __name__ == "__main__":
    # 1. Hitung Tabel Keberhasilan & Loss untuk kedua arah[cite: 4, 5]
    calculate_success_and_loss("logs/mqtt_pub_sent.csv", "logs/coap_get_recv.csv", "MQTT ke CoAP")
    calculate_success_and_loss("logs/coap_post_sent.csv", "logs/mqtt_sub_recv.csv", "CoAP ke MQTT")
    
    # 2. Grafik Delay Ujung ke Ujung (End-to-End Delay) per menit[cite: 5]
    if os.path.exists("logs/coap_get_recv.csv"):
        df_e2e_mqtt = pd.read_csv("logs/coap_get_recv.csv")
        plot_per_minute(
            df_e2e_mqtt, "sent_ts", "e2e_delay",
            "Rerata Delay Ujung ke Ujung (MQTT ke CoAP) per Menit",
            "Rerata Delay (Detik)", "grafik_e2e_mqtt_to_coap.png"
        )
        
    if os.path.exists("logs/mqtt_sub_recv.csv"):
        df_e2e_coap = pd.read_csv("logs/mqtt_sub_recv.csv")
        plot_per_minute(
            df_e2e_coap, "sent_ts", "e2e_delay",
            "Rerata Delay Ujung ke Ujung (CoAP ke MQTT) per Menit",
            "Rerata Delay (Detik)", "grafik_e2e_coap_to_mqtt.png"
        )

    # 3. Grafik Delay Konversi per menit (jika log dari Anggota A tersedia)[cite: 5]
    if os.path.exists("logs/conversion_delay.csv"):
        df_conv = pd.read_csv("logs/conversion_delay.csv")
        plot_per_minute(
            df_conv, "timestamp", "conversion_delay",
            "Rerata Delay Konversi pada Conversion Server per Menit",
            "Delay Konversi (Detik)", "grafik_conversion_delay.png"
        )

    # 4. Grafik Penggunaan CPU Conversion Server per menit[cite: 5]
    if os.path.exists("logs/resource_usage.csv"):
        df_res = pd.read_csv("logs/resource_usage.csv")
        plot_per_minute(
            df_res, "timestamp", "cpu_percent",
            "Rerata Penggunaan CPU Conversion Server per Menit",
            "Penggunaan CPU (%)", "grafik_cpu_usage.png"
        )