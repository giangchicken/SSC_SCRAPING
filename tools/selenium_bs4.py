import json
from selenium import webdriver  
from selenium.webdriver.common.by import By
import time
import pandas as pd
import os
from tqdm import tqdm
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup

class FinanceTableCrawler:
    def __init__(self, config_file, use_headless=True, sleep=3):
        with open(config_file, 'r') as f:
            self.config = json.load(f)
        self.use_headless = use_headless
        self.sleep = sleep
        self.driver = None

    def setup_browser(self, download_dir):
        chrome_options = Options()
        chrome_prefs = {
            "profile.default_content_settings.popups": 0,
            "download.prompt_for_download": False,
            "download.default_directory": download_dir,
            "safebrowsing.enabled": True,
            "profile.default_content_setting_values.automatic_downloads": 1
        }
        chrome_options.add_experimental_option("prefs", chrome_prefs)
        chrome_options.add_experimental_option("detach", True)
        
        if self.use_headless:
            chrome_options.add_argument("--headless")

        self.driver = webdriver.Chrome(options=chrome_options)
        self.driver.get(self.config["base_url"])

    def get_visible_text(self, element):
        # Tìm và loại bỏ tất cả các thẻ <span> có style="display:none"
        for hidden in element.find_all("span", style="display:none"):
            hidden.decompose()  # Loại bỏ thẻ khỏi cây HTML

        # Trả về text của phần tử sau khi đã loại bỏ các thẻ <span> bị ẩn
        return element.get_text(strip=True)

    def get_table(self, soup):
        header_class = self.config["element_ids"]["header_class"]
        table_class = self.config["element_ids"]["table_class"]
        row_class = self.config["element_ids"]["row_class"]

        fi_table = soup.find(class_=header_class)
        headers = [th.get_text(strip=True) for th in fi_table.find_all("th", attrs={'scope': 'col'})]
        # print(headers)
        # Lấy các hàng dữ liệu (rows)
        fi_table_rows = soup.find(class_=table_class)
        rows = fi_table_rows.find_all(class_=row_class)

        table_values = []
        for row in rows:
            values = [self.get_visible_text(span) for span in row.find_all("span") if self.get_visible_text(span)]
            table_values.append(values)

        # Đảm bảo số lượng cột trong mỗi hàng khớp với header
        for x in table_values:
            while len(x) < len(headers):
                x.append("")

        # Tạo DataFrame
        df = pd.DataFrame(table_values, columns=headers)
        return df
    
    def get_firm_information(self, soup):
        ma_doanh_nghiep = soup.find(id=self.config["element_ids"]["company_code_id"]).find(class_=self.config["element_ids"]["company_code_CLS"]).text
        ten_cong_ty = soup.find(id=self.config["element_ids"]["company_name_id"]).find(class_=self.config["element_ids"]["company_name_CLS"]).text
        tieu_de = soup.find(id=self.config["element_ids"]["title_id"]).find(class_=self.config["element_ids"]["title_CLS"]).text

        return ma_doanh_nghiep, ten_cong_ty, tieu_de

    def check_directory(self, output_dir):
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

    def crawl_financial_data(self, ma_ck, df, tables):
        
        e = None
        if len(df) >0:
            checkpoint = df["Thời gian gửi"]
        else:
            checkpoint = []

        default_dir = self.config["output_dir"]
        download_dir = os.path.join(default_dir, ma_ck)
        self.check_directory(download_dir)
        self.setup_browser(download_dir)
        wait = WebDriverWait(self.driver, 10)

        element_ids = self.config["element_ids"]

        # Fill in filters
        search_box = self.driver.find_element(By.NAME, element_ids["search_box"])
        search_box.send_keys(ma_ck)
        self.driver.find_element(By.NAME, element_ids["start_date"]).send_keys(self.config["start_date"])
        self.driver.find_element(By.NAME, element_ids["end_date"]).send_keys(self.config["end_date"])
        self.driver.find_element(By.ID, element_ids["search_button"]).click()
        time.sleep(self.sleep)

        # Data extraction setup
        columns = ["STT", "Tên báo cáo", "Đơn vị", "Trích yếu", "Thời gian gửi", "Mã doanh nghiệp", "Tên công ty", "Tiêu đề"]
        data = {col: [] for col in columns}
        
        check = 0
        new_checkpoint = checkpoint.copy()
        
        while True:
            try:
                try:
                    table = wait.until(
                            EC.presence_of_element_located((By.ID, element_ids["table"])))
                except:
                    self.driver.get(current_url)
                    time.sleep(self.sleep)
                table = wait.until(EC.presence_of_element_located((By.ID, element_ids["table"])))
                rows = table.find_elements(By.TAG_NAME, "tr")
                cols = rows[check].find_elements(By.TAG_NAME, "td")

                Thoi_gian_gui = list(data["Thời gian gửi"])

                if (len(cols) > 3) & (len(cols) <= 6) and (cols[4].text not in Thoi_gian_gui) and (len(cols[3].text) > 1) and (cols[4].text not in new_checkpoint) and (cols[4].text not in checkpoint):
                    data["STT"].append(cols[0].text)
                    data["Tên báo cáo"].append(cols[1].text)
                    data["Đơn vị"].append(cols[2].text)
                    data["Trích yếu"].append(cols[3].text)
                    data["Thời gian gửi"].append(cols[4].text)

                    new_checkpoint.append(cols[4].text)
                    current_url = self.driver.current_url
                    
                    # Additional information
                    cols[1].click()

                    wait.until(EC.presence_of_element_located((By.ID, element_ids["company_code_id"])))

                    full_html = self.driver.page_source
                    soup = BeautifulSoup(full_html, 'html.parser')

                    ma_doanh_nghiep, ten_cong_ty, tieu_de = self.get_firm_information(soup)

                    data["Mã doanh nghiệp"].append(ma_doanh_nghiep)
                    data["Tên công ty"].append(ten_cong_ty)
                    data["Tiêu đề"].append(tieu_de)
                    
                    list_df = []
                    for table_id in element_ids["financial_tables"]:
                        indexs = wait.until(EC.presence_of_element_located((By.ID, table_id)))
                        time.sleep(self.sleep)
                        indexs.click()
                        time.sleep(self.sleep)
                        table_html = self.driver.page_source
                        soup = BeautifulSoup(table_html, 'html.parser')

                        list_df.append(self.get_table(soup))

                    tables[data["Trích yếu"][-1]] = list_df
                    
                    self.driver.get(current_url)
                    check = 0
                    time.sleep(self.sleep)

                check += 1
                if check == len(rows):
                    next_button = self.driver.find_element(By.CLASS_NAME, element_ids["next_button_class"])
                    if "Disabled" in next_button.get_attribute("class"):
                        break
                    next_button.click()
                    time.sleep(self.sleep)
                    check = 0

            except Exception as e:
                print("Error occurred:", e)
                break
        self.driver.quit()

        min_len = min(len(data["STT"]), len(data["Tên báo cáo"]), len(data["Đơn vị"]), len(data["Trích yếu"]), 
              len(data["Thời gian gửi"]), len(data["Mã doanh nghiệp"]), len(data["Tên công ty"]), len(data["Tiêu đề"]))

        data = {col: data[col][:min_len] for col in columns}
        # new_checkpoint = data["Thời gian gửi"]
        # checkpoint += new_checkpoint

        return pd.DataFrame(data), tables, e

    def run_crawler(self, symbols, tables):
        df = pd.DataFrame([])
        e = None
        for ma_ck in tqdm(symbols):
            try:
                print(f"Crawling {ma_ck}")
                retries, max_retries = 0, 10

                while (not e) and (retries < max_retries):
                    retries += 1
                    df_sub, tables, e = self.crawl_financial_data(ma_ck, df, tables)
                    df = pd.concat([df, df_sub], ignore_index=True)

                    if e:
                        print(f"Retrying {ma_ck} due to error:", e)
                        time.sleep(self.sleep)

            except Exception as e:
                print(f"Error with symbol {ma_ck}:", e)
                continue

        return df, tables

