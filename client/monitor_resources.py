# monitor_resources.py
import psutil
import time
import numpy as np
from logger_utils import init_csv, log_event

def find_conversion_server_pid():
    """Mencari PID dari proses python yang menjalankan conversion_server.py"""
    for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
        try:
            cmdline = proc.info.get('cmdline')
            if cmdline and any('conversion_server.py' in arg for arg in cmdline):
                return proc.info['pid']
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            continue
    return None

def monitor_process(duration_sec=600, interval_sec=1):
    pid = find_conversion_server_pid()
    if not pid:
        print("[Monitor] Error: Proses conversion_server.py tidak ditemukan! Pastikan server sudah jalan.")
        return

    proc = psutil.Process(pid)
    log_file = init_csv("resource_usage.csv", ["timestamp", "elapsed_sec", "minute", "cpu_percent", "memory_mb"])
    
    print(f"[Monitor] Memantau PID {pid} (conversion_server.py) selama {duration_sec} detik...")
    cpu_samples = []
    mem_samples = []
    start_time = time.time()
    
    # Panggilan awal cpu_percent agar pembacaan berikutnya akurat
    proc.cpu_percent(interval=None)
    
    while (time.time() - start_time) < duration_sec:
        time.sleep(interval_sec)
        try:
            now = time.time()
            elapsed = now - start_time
            minute = int(elapsed // 60) + 1
            
            cpu = proc.cpu_percent(interval=None)
            mem_mb = proc.memory_info().rss / (1024 * 1024)
            
            cpu_samples.append(cpu)
            mem_samples.append(mem_mb)
            
            log_event(log_file, [now, round(elapsed, 2), minute, cpu, round(mem_mb, 2)])
            print(f"[Menit {minute}] CPU: {cpu}% | RAM: {mem_mb:.2f} MB")
            
        except psutil.NoSuchProcess:
            print("[Monitor] Proses conversion_server.py berhenti di tengah jalan!")
            break

    # Hitung ringkasan statistik wajib
    if cpu_samples:
        print("\n=== RINGKASAN SUMBER DAYA CONVERSION SERVER ===")
        print(f"CPU Rerata        : {np.mean(cpu_samples):.2f}%")
        print(f"CPU Puncak (Max)  : {np.max(cpu_samples):.2f}%")
        print(f"CPU Persentil-95  : {np.percentile(cpu_samples, 95):.2f}%")
        print("-" * 45)
        print(f"RAM Rerata        : {np.mean(mem_samples):.2f} MB")
        print(f"RAM Puncak (Max)  : {np.max(mem_samples):.2f} MB")
        print(f"RAM Persentil-95  : {np.percentile(mem_samples, 95):.2f} MB")

if __name__ == "__main__":
    # Ubah duration_sec menjadi 600 untuk pengujian penuh 10 menit
    monitor_process(duration_sec=600, interval_sec=1)