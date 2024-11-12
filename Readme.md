# Web Scraping Financial Data from SSC using Playwright

## System Requirements
- Python 3.7 or higher
- pip (Python package installer)


## Environment Setup
### 1. **Clone the repository** and navigate into the project directory:

```bash
   git clone https://github.com/giangchicken/SSC_SCRAPING.git
   cd SSC_SCRAPING
```
### 2. **Install Required Packages**
```bash
    pip install -r requirements.txt
```
### 3. **Install Playwright Browsers**      
```bash
    python -m playwright install
    python -m playwright install chromium
```

## Performance Comparison
To evaluate the efficiency of different libraries for web scraping, I tested the script using three methods: **Selenium**, **Selenium with BeautifulSoup**, and **Playwright**. The results are as follows:

| Method                   | Time Taken (Avg) | Nb firm | Description 
| :----------------------- | :--------------: | :-----: | ---------------------------------------------------------------------------------------------: |
| Selenium                 |   74 minutes     |     1   | Used Selenium to navigate and interact with the SSC website to scrape the financial tables.    |
| Selenium + BeautifulSoup |   52 minutes     |     1   | Combined Selenium with BeautifulSoup for parsing the page source, reducing interaction time.   |
| Playwright               |   45 minutes     |     1   | Used Playwright to navigate and interact with the SSC website to scrape the financial tables.  |

## Sequence and multi-processing Performance Comparison

| Method                         | Time Taken (Avg) | Nb firm | Memory Usage (Avg) |
| :----------------------------- | :--------------: | :-----: | ------------------:|
| Sequence                       |   230 minutes    |     5   |         88MB       |
| multi-processing (5 processes) |   45 minutes     |     5   |        450MB       |

## Guidelines
### 1. **Edit Configuration (config_playwright.json)** 
Open **config_playwright.json** in the **tools** folder. This file will include essential configurations for the **FinanceCrawler** class, such as base URL, browser settings, max retries, and storage folder.

```json
    {
    "url": "https://congbothongtin.ssc.gov.vn",
    "start_date" : "01/01/2000",
    "end_date" : "01/01/2025",
    "type_report" : "Báo cáo tài chính Hợp nhất - Năm",
    "elements": {
        "search_box": "#pt9\\:it8112\\:\\:content",           
        "start_date": "#pt9\\:id1\\:\\:content",                           
        "end_date": "#pt9\\:id2\\:\\:content",                              
        "search_button": "#pt9\\:b1\\:\\:content",                         
        "table": "#pt9\\:t1",                                
        "company_code_id": "#pt2\\:plam1",                  
        "company_name_id": "#pt2\\:plam3",                    
        "title_id": "#pt2\\:plam2",                            
        "company_code_CLS": ".xth.xtk",                      
        "company_name_CLS": ".xth.xtk",                      
        "title_CLS": ".xth.xtk",                              
        "financial_tables": ["#pt2\\:BCDKT\\:\\:disAcr", "#pt2\\:KQKD\\:\\:disAcr", "#pt2\\:LCTT-TT\\:\\:disAcr", "#pt2\\:LCTT-GT\\:\\:disAcr"],
        "next_button_class": "#pt9\\:t1\\:\\:nb_nx",
        "total_reports": "#pt9\\:t1\\:\\:nb_rng",                     
        "header_class": "x150",                              
        "table_class": "x13b",                            
        "row_class": "x221",
        "page": "#pt9\\:t1\\:\\:nb_pg"                                
    },
    "output_dir" : "./crawl_table",
    "logging_dir": "E:/PUBLIC_DATA_PROJECT/Optimizing_scraping/logging",
    "max_retries": 10
    }
```

### 2. **Create symbols file** 
Ensure a symbols file exists, e.g., symbols.csv, with stock symbols to be scraped.

### 3. **Set up parameters for multi-processing**
Use the following parameters when running the script: 
```
    parser = argparse.ArgumentParser(description="Run financial data scraping with Playwright.")
    parser.add_argument('--symbols_file', type=str, default='symbols.csv', help="Path to the CSV file containing stock symbols.")
    parser.add_argument('--timeout_per_process', type=int, default=3200, help="Limit time for each process.")
    parser.add_argument('--logtime', type=int, default=200, help="Interval time for logging.")
```


### 4. Run the script
Use the following command to run the scraping script:
```
    python .\playwright_multiprocessing.py --timeout_per_process 3200 --symbols_file symbols.csv 
```

### 5. **Erroneous scraped symbols checking and re-scraping**:
f any errors occur during scraping, use the **check_warning.ipynb** notebook to identify problematic symbols, and create an error_symbols.csv file with symbols requiring re-scraping. Run **check_warning.ipynb**  and the script again to re-scrape:
 
```
    python .\playwright_multiprocessing.py --timeout_per_process 3200 --symbols_file error_symbols.csv
```