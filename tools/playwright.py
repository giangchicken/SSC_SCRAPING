from playwright.sync_api import sync_playwright
from multiprocessing import Process, Queue
import pandas as pd
import time
import re
import json
import os
import re
import logging
from datetime import datetime

class FinanceCrawler:
    def __init__(self, config, symbol, sleep=3):
        self.config = config
        self.symbol = symbol
        # self.output_queue = output_queue
        self.checkpoint_file = f"./checkpoints/checkpoint_{self.symbol}.json"
        self.browser = None
        self.page = None
        self.retry_attempts = 0
        self.max_retries = config["max_retries"]
        self.checkpoint = self.load_checkpoint()
        self.sleep = sleep
        self.logging_dir = config["logging_dir"]
        self._setup_logging()

    def setup_browser(self, playwright):
        self.browser = playwright.chromium.launch(headless=False)
        self.page = self.browser.new_page()
        self.page.goto(self.config['url'])
        self.page.wait_for_selector(self.config["elements"]["search_box"])

    def search(self):
        self.page.locator(self.config["elements"]["search_box"]).fill(self.symbol)
        self.page.locator(self.config["elements"]["start_date"]).fill(self.config['start_date'])
        self.page.locator(self.config["elements"]["end_date"]).fill(self.config['end_date'])
        self.page.get_by_role("button", name="Tìm kiếm").click(force=True)
        time.sleep(self.sleep)

    def clean_text(self, text):
        return re.sub(r'[\n\t]+', ',', text).strip(',')

    def convert_to_dataframe(self, header, rows):
        num_columns = len(header)
        formatted_rows = [rows[i:i + num_columns] for i in range(0, len(rows), num_columns)]
        return pd.DataFrame(formatted_rows, columns=header)

    # def go_to_page(self, page_number):
    #     page_button = self.page.locator(f'{self.config["elements"]["page"]}{(page_number - 1) * 15}')
    #     if page_button.is_visible() and page_button.is_enabled():
    #         page_button.click()
    #         time.sleep(self.sleep)
    def go_to_page(self, target_page_number):
        self.page.wait_for_selector(self.config["elements"]["next_button_class"])
        current_page = 1
        while current_page < target_page_number:
            self.page.wait_for_selector(self.config["elements"]["next_button_class"])
            # Define the "Next Page" button locator
            next_button = self.page.locator(self.config["elements"]["next_button_class"])
            
            # Check if the Next Page button is visible and enabled
            if next_button.is_visible() and next_button.is_enabled():
                next_button.click()
                time.sleep(self.sleep)  # Pause to allow the page to load
                current_page += 1
                # print(f"Navigated to page {current_page}")
            else:
                print("Next button is not visible or enabled. Stopping navigation.")
                break
    def get_table(self):
        headers = self.page.locator(f'.{self.config["elements"]["table_class"]}').locator(f'[role="columnheader"].{self.config["elements"]["header_class"]}').all_inner_texts()
        rows = self.page.locator(f'.{self.config["elements"]["table_class"]}').locator('[role="row"]').locator(f'.{self.config["elements"]["row_class"]}').all_inner_texts()
        return self.convert_to_dataframe(headers, rows)

    def extract_report_details(self, row_data):
        # Lấy mã doanh nghiệp, tên công ty, tiêu đề báo cáo
        row_data.append(self.page.locator(self.config["elements"]["company_code_id"]).locator(self.config["elements"]["company_code_CLS"]).inner_text())
        row_data.append(self.page.locator(self.config["elements"]["company_name_id"]).locator(self.config["elements"]["company_name_CLS"]).inner_text())
        row_data.append(self.page.locator(self.config["elements"]["title_id"]).locator(self.config["elements"]["title_CLS"]).inner_text())
        
        # Duyệt qua từng bảng tài chính và lưu ngay sau khi thu thập
        list_df = []
        for table_id in self.config["elements"]["financial_tables"]:
            self.page.locator(table_id).click(force=True)
            time.sleep(self.sleep)
            df_sub = self.get_table()
            list_df.append(df_sub)

        # Lưu các bảng tài chính ngay sau khi thu thập
        report_dir = self.save_report_details(row_data[3], row_data[4], list_df, row_data[1])
        row_data.append(report_dir)
        return list_df

    def sanitize_filename(self, filename):
        # Thay thế các ký tự không hợp lệ bằng dấu gạch dưới
        return re.sub(r'[<>:"/\\|?*]', '_', filename)
    
    def save_report_details(self, title, date, tables, backup):
        safe_title = self.sanitize_filename(title)
        safe_date = self.sanitize_filename(date)
        if len(title) > 130:
            safe_title = self.sanitize_filename(backup)
        # Tạo thư mục để lưu các báo cáo tài chính
        report_dir = f"{self.config['output_dir']}/{self.symbol}/{safe_title}_{safe_date}"
        os.makedirs(report_dir, exist_ok=True)
        filenames = ["BCDKT.csv", "KQKT.csv", "LCTT_TT1.csv", "LCTT_TT2.csv"]
        
        for df, filename in zip(tables, filenames):
            file_path = os.path.join(report_dir, filename)
            if not os.path.exists(file_path):
                df.to_csv(file_path, index=False)
                # print(f"Đã lưu {file_path}")
            # else:
            #     print(f"Tệp {file_path} đã tồn tại, bỏ qua.")
        return report_dir

    def save_checkpoint(self, checkpoint):
        with open(self.checkpoint_file, "w") as file:
            json.dump(checkpoint, file)

    def load_checkpoint(self):
        try:
            with open(self.checkpoint_file, "r") as file:
                return json.load(file)
        except FileNotFoundError:
            return {"current_page": 1, "last_row_index": 0}
    
    def _setup_logging(self):
        # Ensure the logging directory exists
        os.makedirs("logging", exist_ok=True)

        # Generate a unique log filename with timestamp
        log_filename = f"{self.logging_dir}/{datetime.now().strftime('%Y%m%d_%H%M%S')}_crawl.log"

        # Set up logging configuration
        logging.basicConfig(
            filename=log_filename,
            level=logging.INFO,
            format="%(asctime)s - %(levelname)s - %(message)s"
        )

        self.logger = logging.getLogger(__name__)


    def crawl(self):
        self.search()
        total_reports_text = self.page.locator(self.config["elements"]["total_reports"]).inner_text()
        numbers = re.findall(r'\d+', total_reports_text)
        number_reports = int(numbers[2])
        number_pages = int(number_reports) // int(numbers[1]) + 1

        columns = ["STT", "Tên báo cáo", "Đơn vị", "Trích yếu", "Thời gian gửi", "Mã doanh nghiệp", "Tên công ty", "Tiêu đề", "Saving_path"]
        data = {col: [] for col in columns}

        current_page = self.checkpoint["current_page"]
        last_row_index = self.checkpoint["last_row_index"]
        self.go_to_page(current_page)

        while current_page <= number_pages:
            try:
                print(f"Bot scraping {self.symbol} at page {current_page}/{number_pages}")
                self.page.wait_for_selector(self.config["elements"]["table"])
                headers = self.page.get_by_role("columnheader").all_inner_texts()
                column_indices = [headers.index(col) for col in columns if col in headers]

                rows = self.page.get_by_role("row").all()
                for row_index in range(last_row_index, len(rows)):
                    row = rows[row_index]
                    row_texts = self.clean_text(row.all_inner_texts()[0]).split(",")
                    row_data = [row_texts[i] if i < len(row_texts) else "" for i in column_indices]

                    if (not row_data[1] in headers) and ("Filter" not in row_data[1]):
                        try:
                            row.get_by_text(row_data[column_indices[1]]).click(force=True)
                        except:
                            row.get_by_role("link", name = row_data[column_indices[1]]).click(force=True)

                        self.page.wait_for_selector(self.config["elements"]["company_code_id"])

                        list_df = self.extract_report_details(row_data)

                        # for col, val in zip(columns, row_data):
                        #     data[col].append(val)

                        # # Lưu checkpoint sau mỗi hàng
                        # self.checkpoint["last_row_index"] = row_index + 1
                        # self.save_checkpoint(self.checkpoint)

                        # Quay lại bảng
                        time.sleep(self.sleep)
                        self.page.go_back()
                        time.sleep(self.sleep)
                        self.go_to_page(current_page)
                        time.sleep(self.sleep)

                        # Lưu checkpoint sau mỗi hàng
                        self.checkpoint["last_row_index"] = row_index + 1
                        self.save_checkpoint(self.checkpoint)

                        for col, val in zip(columns, row_data):
                            data[col].append(val)

                    self.checkpoint["last_row_index"] = row_index + 1
                # Cập nhật checkpoint khi chuyển trang
                current_page += 1
                self.checkpoint["current_page"] = current_page
                self.checkpoint["last_row_index"] = 0
                self.save_checkpoint(self.checkpoint)

                # Chuyển sang trang tiếp theo nếu còn
                if current_page <= number_pages:
                    next_button = self.page.locator(self.config["elements"]["next_button_class"])
                    if next_button.is_enabled():
                        next_button.click(force=True)
                        time.sleep(self.sleep)
                    else:
                        print("Can't access Next page. Stop.")
                        break

            except Exception as e:
                print(f"Error at page {current_page}, row {row_index}: {e}")
                self.retry_attempts += 1
                if self.retry_attempts < self.max_retries:
                    print(f"Retrying ({self.retry_attempts}/{self.max_retries})...")
                    self.page.go_back()
                    time.sleep(self.sleep)
                    
                else:
                    print("Max retries reached. Exiting...")
                    break
        # Kiểm tra số lượng báo cáo đã crawl được
        self.verify_report_count(data, number_reports)

        # self.output_queue.put(data)

    def verify_report_count(self, data, number_reports):
        symbol_dir = os.path.join(self.config['output_dir'], self.symbol)

        df = pd.DataFrame(data)
        # Đếm số lượng thư mục con trong symbol_dir
        if os.path.exists(symbol_dir):
            crawled_report_count = len([d for d in os.listdir(symbol_dir) if os.path.isdir(os.path.join(symbol_dir, d))])
        else:
            crawled_report_count = 0  # Nếu thư mục không tồn tại, coi như chưa crawl được gì

        if crawled_report_count == number_reports:
            message = f"Scraping Successfully {self.symbol}: {crawled_report_count}/{number_reports}"
            self.logger.info(message)
            print(message)
            df.to_csv(f"{self.config['output_dir']}/{self.symbol}/success.csv")
            return True
        else:
            message = f"Not enough reports {self.symbol}: {crawled_report_count}/{number_reports}"
            self.logger.warning(message)
            print(message)
            df.to_csv(f"{self.config['output_dir']}/{self.symbol}/warning.csv")
            return False
            
    def close(self):
        self.browser.close()

# def worker(config, symbol, output_queue):
#     with sync_playwright() as playwright:
#         crawler = FinanceCrawler(config, symbol, output_queue)
#         crawler.setup_browser(playwright)
#         crawler.crawl()
#         crawler.close()

# def run_multiprocessing(symbols, config):
#     output_queue = Queue()
#     processes = []

#     for symbol in symbols:
#         p = Process(target=worker, args=(config, symbol, output_queue))
#         p.start()
#         processes.append(p)

#     # Đợi các tiến trình hoàn tất
#     for p in processes:
#         p.join()

# with open('./tools/config_playwright.json', 'r') as file:
#     config = json.load(file)

# symbols = ["VIC", "HPG", "MSN"]

# if __name__ == "__main__":
#     start_time = time.time()
#     run_multiprocessing(symbols, config)
#     print("Scraping time: ", time.time() - start_time)
