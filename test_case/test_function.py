import re
import os
import pandas as pd
import numpy as np
import json
from tqdm import tqdm

def extract_symbols(log_folder_path):
    warning_symbols = set()
    info_symbols = set()

    # Regex patterns to identify WARNING and INFO logs
    warning_pattern = re.compile(r"WARNING - Not enough reports (\w+):")
    info_pattern = re.compile(r"INFO - Scraping Successfully (\w+):")

    # Loop through each log file in the folder
    for filename in os.listdir(log_folder_path):
        if filename.endswith(".log"):
            with open(os.path.join(log_folder_path, filename), 'r') as file:
                for line in file:
                    # Match WARNING logs
                    warning_match = warning_pattern.search(line)
                    if warning_match:
                        symbol = warning_match.group(1)
                        warning_symbols.add(symbol)

                    # Match INFO logs
                    info_match = info_pattern.search(line)
                    if info_match:
                        symbol = info_match.group(1)
                        info_symbols.add(symbol)

    # Exclude symbols that appear in INFO logs from WARNING symbols
    final_warning_symbols = warning_symbols - info_symbols
    # print("Total Scraped symbols: ", len(list(final_warning_symbols)) + len(list(info_symbols)))
    info_symbols1 = info_symbols.copy()
    # Check folders in crawl_table for symbols in info_symbols
    for symbol in info_symbols:
        try:
            symbol_path = os.path.join('./crawl_table/', symbol)
            success_df = pd.read_csv(symbol_path + "/success.csv")
            if len(success_df) != len(success_df["STT"].unique()):
                final_warning_symbols.add(symbol)
                info_symbols1.remove(symbol)
                # print("duplicate in success symbols: ")
                # print(symbol)
        except:
            final_warning_symbols.add(symbol)
            info_symbols1.remove(symbol)
            # print("Dont have success.csv: ")
            # print(symbol)

    # Create a DataFrame to display the results
    warning_df = pd.DataFrame(final_warning_symbols, columns=["symbol"])
    info_df = pd.DataFrame(info_symbols1, columns=["symbol"])
    # print("Symbols with warnings (excluding those in INFO logs):")
    # print(warning_df)

    return warning_df, info_df

# Load configuration
with open('./tools/config_playwright.json', 'r') as file:
    config = json.load(file)

def check_duplicate_content(config):
    # Đường dẫn đến thư mục chứa các stockid
    base_folder = config["output_dir"]

    # Biến đếm
    total_files = 0
    error_files = 0

    # Duyệt qua các thư mục stockid
    for stockid in tqdm(os.listdir(base_folder), desc="Check Duplicate data BCDKT & KQKD"):
        stock_folder = os.path.join(base_folder, stockid)
        
        if not os.path.isdir(stock_folder):
            continue  # Bỏ qua nếu không phải thư mục

        # Đường dẫn đến file success.csv
        success_path = os.path.join(stock_folder, "success.csv")
        
        if not os.path.exists(success_path):
            # print(f"{stockid} khong co success.csv")
            continue  # Bỏ qua nếu không có success.csv

        try:
            df_success = pd.read_csv(success_path)
            # print(df_success)
            
            # Duyệt qua từng saving_path trong success.csv
            for saving_path in df_success["Saving_path"].dropna():
                saving_path = saving_path[13:]
                # print(source + saving_path)
                kqkt_path = os.path.join(base_folder + saving_path, "KQKD.csv")
                
                if not os.path.exists(kqkt_path):
                    continue  # Bỏ qua nếu không có file KQKT.csv
                
                total_files += 1

                try:
                    df_kqkt = pd.read_csv(kqkt_path, encoding="utf-8", header=None)
                    
                    # Kiểm tra xem "TÀI SẢN NGẮN HẠN" có trong cột đầu tiên không
                    if df_kqkt.iloc[:, 0].astype(str).str.contains("TÀI SẢN NGẮN HẠN", na=False).any():
                        error_files += 1  # Tăng số lần sai

                except Exception as e:
                    print(f"Lỗi đọc file {kqkt_path}: {e}")

        except Exception as e:
            print(f"Lỗi đọc file {success_path}: {e}")

    # Tính tỷ lệ %
    error_rate = (error_files / total_files) * 100 if total_files > 0 else 0

    # print(f"Tổng số file: {total_files}")
    # print(f"Số file bị lỗi: {error_files}")
    # print(f"Tỷ lệ lỗi: {error_rate:.2f}%")

    return total_files, error_files, error_rate


import os
import pandas as pd

# Function to check duplicate financial tables in each stock folder
def check_duplicate_financial_tables(OUTPUT_DIR):
    total_duplicates = 0  # Tổng số báo cáo bị trùng
    
    # Lặp qua từng mã chứng khoán (folder trong OUTPUT_DIR)
    for stock_code in tqdm(os.listdir(OUTPUT_DIR), desc="Check Duplicate data in success.csv"):
        stock_folder = os.path.join(OUTPUT_DIR, stock_code)
        success_file = os.path.join(stock_folder, "success.csv")

        if not os.path.exists(success_file):
            continue  # Bỏ qua nếu không có success.csv
        
        df_success = pd.read_csv(success_file)
        stock_duplicate_count = 0  # Số trùng lặp trong folder này

        for _, row in df_success.iterrows():
            report_dir = row["Saving_path"]  # Cột chứa đường dẫn thư mục báo cáo
            
            if os.path.exists(report_dir):
                file_paths = {
                    "BCDKT": os.path.join(report_dir, "BCDKT.csv"),
                    "KQKD": os.path.join(report_dir, "KQKD.csv"),
                    "LCTT_TT": os.path.join(report_dir, "LCTT_TT.csv"),
                    "LCTT_GT": os.path.join(report_dir, "LCTT_GT.csv"),
                }
                
                # Đọc file CSV nếu tồn tại
                tables = {}
                for key, path in file_paths.items():
                    if os.path.exists(path):
                        try:
                            tables[key] = pd.read_csv(path)
                        except Exception as e:
                            print(f"⚠️ Lỗi đọc file {path}: {e}")

                # Kiểm tra trùng lặp giữa các bảng trong cùng một báo cáo
                keys = list(tables.keys())
                for i in range(len(keys)):
                    for j in range(i + 1, len(keys)):
                        if keys[i] in tables and keys[j] in tables:
                            if tables[keys[i]].equals(tables[keys[j]]):
                                stock_duplicate_count += 1
                                # break  # Nếu đã tìm thấy trùng thì không cần kiểm tra thêm
            
        total_duplicates += stock_duplicate_count
        if stock_duplicate_count > 0:
            print(f"✅ {stock_code}: {stock_duplicate_count} duplicate reports")  # Log từng folder

    return total_duplicates

