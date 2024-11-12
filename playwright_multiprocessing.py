from playwright.sync_api import sync_playwright
from multiprocessing import Process, Queue
from tools.playwright import FinanceCrawler
import psutil
import signal
import json
import time
import pandas as pd
import os
from datetime import datetime
import argparse

class ProcessManager:
    def __init__(self, symbol, config, timeout):
        self.symbol = symbol
        self.config = config
        self.timeout = timeout
        self.process = None
        self.start_time = None
        
    def worker(self):
        try:
            with sync_playwright() as playwright:
                crawler = FinanceCrawler(self.config, self.symbol, 3)
                crawler.setup_browser(playwright)
                crawler.crawl()
                crawler.close()
        except Exception as e:
            print(f"Error in worker process for symbol {self.symbol}: {e}")
            
    def start_process(self):
        """Khởi động process và ghi nhận thời gian bắt đầu"""
        self.process = Process(target=self.worker)
        self.process.start()
        self.start_time = datetime.now()
        return self.process.pid
        
    def check_timeout(self):
        """Kiểm tra xem process có vượt quá thời gian cho phép không"""
        if self.start_time:
            elapsed_time = (datetime.now() - self.start_time).total_seconds()
            return elapsed_time > self.timeout
        return False
        
    def get_process_info(self):
        """Lấy thông tin chi tiết về process"""
        if self.process and self.process.is_alive():
            try:
                p = psutil.Process(self.process.pid)
                return {
                    'pid': self.process.pid,
                    'symbol': self.symbol,
                    'cpu_percent': p.cpu_percent(),
                    'memory_mb': p.memory_info().rss / 1024 / 1024,
                    'status': p.status(),
                    'runtime': (datetime.now() - self.start_time).total_seconds()
                }
            except:
                return None
        return None

    def terminate_process(self):
        """Kết thúc process với thông tin chi tiết"""
        if self.process and self.process.is_alive():
            pid = self.process.pid
            try:
                p = psutil.Process(pid)
                info = self.get_process_info()
                
                print(f"\nĐang kết thúc process cho symbol {self.symbol} (PID: {pid})")
                if info:
                    print(f"Thông tin process trước khi kết thúc:")
                    print(f"- Thời gian chạy: {info['runtime']:.2f} giây")
                    print(f"- CPU Usage: {info['cpu_percent']}%")
                    print(f"- Memory Usage: {info['memory_mb']:.2f} MB")
                    print(f"- Status: {info['status']}")
                
                # Thử kết thúc "nhẹ nhàng" trước
                self.process.terminate()
                self.process.join(timeout=3)
                
                # Nếu process vẫn còn sống, buộc kết thúc
                if self.process.is_alive():
                    print(f"Process {pid} không phản hồi SIGTERM, đang sử dụng SIGKILL...")
                    self.process.kill()
                
                print(f"Đã kết thúc process {pid} thành công")
                
            except psutil.NoSuchProcess:
                print(f"Process {pid} không tồn tại")
            except Exception as e:
                print(f"Lỗi khi kết thúc process {pid}: {e}")

def run_multiprocessing(symbols, config, timeout_per_process=600, time_log=200):
    # Khởi tạo danh sách process managers
    process_managers = [ProcessManager(symbol, config, timeout_per_process) 
                       for symbol in symbols]
    
    # Khởi động tất cả các process
    for pm in process_managers:
        pid = pm.start_process()
        print(f"Đã khởi động process cho symbol {pm.symbol} với PID {pid}")
    
    try:
        while True:
            # Kiểm tra và in thông tin của từng process
            active_processes = False
            for pm in process_managers:
                if pm.process and pm.process.is_alive():
                    active_processes = True
                    info = pm.get_process_info()
                    if info:
                        print(f"\nSymbol {pm.symbol} (PID {info['pid']}):")
                        print(f"Runtime: {info['runtime']:.1f}s, CPU: {info['cpu_percent']}%, "
                              f"Memory: {info['memory_mb']:.1f}MB")
                    
                    # Kiểm tra timeout
                    if pm.check_timeout():
                        print(f"\nProcess cho symbol {pm.symbol} đã vượt quá thời gian "
                              f"cho phép ({pm.timeout}s)")
                        pm.terminate_process()
            
            if not active_processes:
                print("\nTất cả các process đã hoàn thành!")
                break
                
            time.sleep(time_log)  # Đợi 200 giây trước khi kiểm tra lại
            
    except KeyboardInterrupt:
        print("\nNhận được tín hiệu dừng từ người dùng (Ctrl+C)")
        # Kết thúc tất cả các process đang chạy
        for pm in process_managers:
            if pm.process and pm.process.is_alive():
                pm.terminate_process()


# Function to read logs, extract messages, and display results
def read_log_and_display_results(log_directory="logging"):
    success_logs = []
    warning_logs = []

    # Find the latest log file in the logging directory
    log_files = sorted(os.listdir(log_directory), reverse=True)
    if log_files:
        latest_log_file = os.path.join(log_directory, log_files[0])
        
        # Read and extract successful and warning crawls
        with open(latest_log_file, "r") as log_file:
            for line in log_file:
                if "Scraping Successfully" in line:
                    parts = line.strip().split()
                    symbol = parts[7].replace(":", "")
                    report_count = parts[8]
                    success_logs.append({"Symbol": symbol, "Report Count": report_count})
                elif "Not enough reports" in line:
                    parts = line.strip().split()
                    symbol = parts[8].replace(":", "")
                    report_count = parts[9]
                    warning_logs.append({"Symbol": symbol, "Report Count": report_count})

    # Convert to DataFrames
    success_df = pd.DataFrame(success_logs)
    warning_df = pd.DataFrame(warning_logs)

    # Remove duplicates where symbols are in both success and warning
    if not success_df.empty and not warning_df.empty:
        warning_df = warning_df[~warning_df['Symbol'].isin(success_df['Symbol'])]

    # Concatenate success and warning DataFrames and display
    final_df = pd.concat([success_df, warning_df], ignore_index=True)
    print(final_df)


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="Run financial data scraping with Playwright.")
    parser.add_argument('--symbols_file', type=str, default='symbols.csv', help="Path to the CSV file containing stock symbols.")
    parser.add_argument('--timeout_per_process', type=int, default=3200, help="Limit time for each process.")
    parser.add_argument('--logtime', type=int, default=200, help="Interval time for logging.")
    args = parser.parse_args()

    # Load configuration
    with open('./tools/config_playwright.json', 'r') as file:
        config = json.load(file)

    symbols_df = pd.read_csv(args.symbols_file)
    symbols = symbols_df['symbol'].tolist()
    split_symbols = [symbols[i:i + 5] for i in range(0, len(symbols), 5)]

    # split_symbols = [["VIC", "HPG", "MSN", "FPT", "VPB"]]

    # Chạy multiprocessing với monitoring
    for sub_symbols in split_symbols:
        start_time = time.time()

        run_multiprocessing(sub_symbols, config, timeout_per_process=args.timeout_per_process, time_log=args.logtime)

        read_log_and_display_results(config["logging_dir"])
        print(f"Tổng thời gian chạy: {time.time() - start_time:.2f} giây")
    