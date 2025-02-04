import streamlit as st
import pandas as pd
import os
import json
import psutil
from tools.playwright import FinanceCrawler
from test_case.test_function import *

# Load configuration
with open('./tools/config_playwright.json', 'r') as file:
    config = json.load(file)

LOG_DIR = config.get("logging_dir", "logging")
OUTPUT_DIR = config.get("output_dir", "./crawl_table")  # Thư mục chứa file đã crawl

# Function to get process statistics
def get_process_stats():
    processes = []
    for proc in psutil.process_iter(['pid', 'name', 'cpu_percent', 'memory_info', 'status']):
        if "python" in proc.info['name'].lower():  # Assuming the crawler runs on Python
            processes.append(proc.info)
    return processes

# Function to count crawled files
def count_crawled_files():
    total_files = 0
    for root, _, files in os.walk(OUTPUT_DIR):
        total_files += len([f for f in files if f.endswith(".csv")])  # Chỉ đếm file CSV
    return total_files

# Function to read logs and calculate success/error rates
def analyze_logs():
    warnings_df, info_df = extract_symbols(LOG_DIR)
    error_count = warnings_df.shape[0] if warnings_df is not None else 0
    success_count = info_df.shape[0] if info_df is not None else 0
    total_reports = error_count + success_count
    error_rate = (error_count / total_reports * 100) if total_reports > 0 else 0
    return success_count, error_count, error_rate

# Streamlit Dashboard
st.title("📊 Financial Data Crawling Dashboard")

# Log analysis
st.header("📑 Log Analysis")
success, error, error_rate = analyze_logs()
st.metric(label="✅ Successful Crawls", value=success)
st.metric(label="❌ Errors Encountered", value=error)
st.metric(label="⚠️ Error Rate (%)", value=f"{error_rate:.2f}%")

# Crawled file count
st.header("📂 Crawled Files")
total_files = count_crawled_files()
st.metric(label="📄 Total Crawled Files", value=total_files)

# Test case
st.header("🔍 Duplicate content KQKD vs BCDKT")
total, error, error_rate = check_duplicate_content(config)
st.metric(label="📊 Total Crawled Reports", value=success)
st.metric(label="⚠️ Errors Reports", value=error)
st.metric(label="❗ Error Rate (%)", value=f"{error_rate:.2f}%")


st.header("🔍 Duplicate Financial Data Check")
duplicate_count = check_duplicate_financial_tables(OUTPUT_DIR)
st.metric(label="⚠️ Duplicate Financial Reports", value=duplicate_count)


# Visualizations
st.header("📈 Crawl Status")
st.bar_chart(pd.DataFrame({"Success": [success], "Errors": [error]}))

# Refresh button
if st.button("🔄 Refresh Data"):
    st.experimental_rerun()
