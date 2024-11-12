from playwright.sync_api import sync_playwright
from multiprocessing import Process, Queue

from tools.playwright import FinanceCrawler
import json
import time
import pandas as pd
import os

def worker(config, symbol):
    with sync_playwright() as playwright:
        crawler = FinanceCrawler(config, symbol, 3)
        crawler.setup_browser(playwright)
        crawler.crawl()
        crawler.close()

def run_multiprocessing(symbols, config, timeout_per_process=600):
    processes = []

    for symbol in symbols:
        p = Process(target=worker, args=(config, symbol))
        p.start()
        processes.append(p)
        print(f"Started process {p.pid} for symbol {symbol}")

    # Monitor processes with a timeout using a while loop
    for p in processes:
        start_time = time.time()
        while p.is_alive():
            elapsed_time = time.time() - start_time
            if elapsed_time > timeout_per_process:
                print(f"Process {p.pid} for symbol {p.name} exceeded timeout. Terminating.")
                p.terminate()
                p.join()
                print(f"Terminated process {p.pid}")
                break
            time.sleep(1)  # Check every second for timeout

    # # Monitor processes with timeout
    # start_time = time.time()
    # for p in processes:
    #     p.join(timeout=timeout_per_process)
    #     if p.is_alive():
    #         print(f"Process {p.pid} for symbol {p.name} is still running. Terminating.")
    #         p.terminate()
    #         p.join()
    #         print(f"Terminated process {p.pid}")

    print("All scraping processes completed.")



if __name__ == "__main__":
    start_time = time.time()

    # Load configuration
    with open('./tools/config_playwright.json', 'r') as file:
        config = json.load(file)

    symbols = ["VIC", "HPG", "MSN"]

    # # Initialize output queue
    # output_queue = Queue()

    # Run multiprocessing with monitoring
    run_multiprocessing(symbols, config, timeout_per_process=3000)  # 50 minutes per process

    # # Optionally, process the results further or save to a file
    # # For example, combining all data into a single DataFrame:
    # combined_data = pd.concat([pd.DataFrame(data) for data in results], ignore_index=True)
    # combined_data.to_csv(os.path.join(config["output_dir"], "financial_reports_combined.csv"), index=False)
    # print(f"Combined data saved to {os.path.join(config['output_dir'], 'financial_reports_combined.csv')}")

    print(f"Total scraping time: {time.time() - start_time} seconds")