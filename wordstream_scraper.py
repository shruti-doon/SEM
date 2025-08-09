
import pandas as pd
import time
import yaml
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from selenium.webdriver.common.keys import Keys
from datetime import datetime
import os
import subprocess

class WordStreamScraper:
    def __init__(self, config_file: str = "config.yaml"):
        self.config = self.load_config(config_file)
        self.base_url = "https://www.wordstream.com/keywords?camplink=homepage&campname=FKT&cid=Web_Any_Products_FreeKeyword_Tool_KWT"
        self.results = []

    def load_config(self, config_file: str):
        with open(config_file, 'r') as file:
            return yaml.safe_load(file)

    def find_chromedriver(self):
        paths = ["/usr/bin/chromedriver", "/usr/local/bin/chromedriver", "/snap/bin/chromedriver", "/usr/bin/chromium-chromedriver"]
        for p in paths:
            if os.path.exists(p):
                return p
        try:
            r = subprocess.run(['which', 'chromedriver'], capture_output=True, text=True)
            if r.returncode == 0:
                return r.stdout.strip()
        except:
            pass
        return None

    def setup_driver(self):
        chrome_options = Options()
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--window-size=1920,1080")
        try:
            chromedriver_path = self.find_chromedriver()
            if chromedriver_path:
                service = Service(chromedriver_path)
                return webdriver.Chrome(service=service, options=chrome_options)
            return webdriver.Chrome(options=chrome_options)
        except Exception:
            return None

    def scrape_keywords(self, website_url: str, country: str = None):
        if country is None:
            country = self.config['service_locations'][0] if self.config['service_locations'] else "United States"
        driver = self.setup_driver()
        if not driver:
            return []
        try:
            driver.get(self.base_url)
            wait = WebDriverWait(driver, 10)
            time.sleep(3)
            try:
                url_input = wait.until(EC.presence_of_element_located((By.NAME, "input_1")))
                time.sleep(2)
                driver.execute_script("arguments[0].value = '';", url_input)
                driver.execute_script("arguments[0].setAttribute('value', '');", url_input)
                time.sleep(1)
                driver.execute_script("arguments[0].value = arguments[1];", url_input, website_url)
                driver.execute_script("arguments[0].setAttribute('value', arguments[1]);", url_input, website_url)
                driver.execute_script("arguments[0].dispatchEvent(new Event('input', { bubbles: true }));", url_input)
                driver.execute_script("arguments[0].dispatchEvent(new Event('change', { bubbles: true }));", url_input)
                driver.execute_script("arguments[0].dispatchEvent(new Event('blur', { bubbles: true }));", url_input)
                driver.execute_script("arguments[0].dispatchEvent(new Event('keyup', { bubbles: true }));", url_input)
                time.sleep(2)
            except TimeoutException:
                return []
            try:
                submit_button = driver.find_element(By.CSS_SELECTOR, "input[type='submit'][value='FIND MY KEYWORDS']")
                driver.execute_script("arguments[0].click();", submit_button)
            except NoSuchElementException:
                return []
            time.sleep(5)
            try:
                dialog_url_input = wait.until(EC.presence_of_element_located((By.NAME, "websiteURLOrKeyword")))
                time.sleep(2)
                driver.execute_script("arguments[0].value = '';", dialog_url_input)
                driver.execute_script("arguments[0].value = arguments[1];", dialog_url_input, website_url)
                driver.execute_script("arguments[0].dispatchEvent(new Event('input', { bubbles: true }));", dialog_url_input)
                driver.execute_script("arguments[0].dispatchEvent(new Event('change', { bubbles: true }));", dialog_url_input)
                time.sleep(1)
            except TimeoutException:
                return []
            try:
                selectors = [
                    "input[aria-label='location']",
                    "input[type='text'][role='combobox']",
                    "input[aria-autocomplete='list']"
                ]
                country_input = None
                for s in selectors:
                    try:
                        country_input = driver.find_element(By.CSS_SELECTOR, s)
                        break
                    except NoSuchElementException:
                        continue
                if country_input:
                    driver.execute_script("arguments[0].click();", country_input)
                    time.sleep(1)
                    driver.execute_script("arguments[0].value = '';", country_input)
                    driver.execute_script("arguments[0].setAttribute('value', '');", country_input)
                    driver.execute_script("arguments[0].value = arguments[1];", country_input, country)
                    driver.execute_script("arguments[0].dispatchEvent(new Event('input', { bubbles: true }));", country_input)
                    time.sleep(2)
                    try:
                        dropdown_option = driver.find_element(By.XPATH, f"//li[contains(text(), '{country}')]")
                        driver.execute_script("arguments[0].click();", dropdown_option)
                    except NoSuchElementException:
                        country_input.send_keys(Keys.ENTER)
                    time.sleep(1)
            except Exception:
                pass
            time.sleep(2)
            try:
                submit_button = driver.find_element(By.CSS_SELECTOR, "[data-testid='buttonContinue']")
                driver.execute_script("arguments[0].click();", submit_button)
            except NoSuchElementException:
                return []
            except Exception:
                return []
            time.sleep(20)
            try:
                wait.until(EC.presence_of_element_located((By.TAG_NAME, "table")))
                keywords_data = self.extract_table_data(driver)
                return keywords_data
            except TimeoutException:
                return []
        except Exception:
            return []
        finally:
            driver.quit()

    def extract_table_data(self, driver):
        keywords_data = []
        try:
            table = driver.find_element(By.TAG_NAME, "table")
            rows = table.find_elements(By.TAG_NAME, "tr")
            for row in rows[1:]:
                try:
                    keyword_cell = row.find_element(By.TAG_NAME, "th")
                    keyword = keyword_cell.text.strip()
                    data_cells = row.find_elements(By.TAG_NAME, "td")
                    if len(data_cells) >= 4:
                        search_volume = self.extract_number(data_cells[0].text.strip())
                        if search_volume >= 500:
                            keywords_data.append({
                                'keyword': keyword,
                                'search_volume': search_volume,
                                'top_of_page_bid_low': self.extract_number(data_cells[1].text.strip()),
                                'top_of_page_bid_high': self.extract_number(data_cells[2].text.strip()),
                                'competition': data_cells[3].text.strip()
                            })
                except Exception:
                    try:
                        all_cells = row.find_elements(By.CSS_SELECTOR, "th, td")
                        if len(all_cells) >= 5:
                            keyword = all_cells[0].text.strip()
                            search_volume = self.extract_number(all_cells[1].text.strip())
                            if search_volume >= 500:
                                keywords_data.append({
                                    'keyword': keyword,
                                    'search_volume': search_volume,
                                    'top_of_page_bid_low': self.extract_number(all_cells[2].text.strip()),
                                    'top_of_page_bid_high': self.extract_number(all_cells[3].text.strip()),
                                    'competition': all_cells[4].text.strip()
                                })
                    except Exception:
                        pass
        except Exception:
            pass
        return keywords_data

    def extract_number(self, text):
        try:
            cleaned = text.replace(',', '').replace('$', '').replace('K', '000').replace('M', '000000')
            return int(float(cleaned))
        except:
            return 0

    def scrape_both_websites(self):
        all_keywords = []
        country = self.config['service_locations'][0] if self.config['service_locations'] else "United States"
        brand_keywords = self.scrape_keywords(self.config['brand_website'], country)
        for kw in brand_keywords:
            kw['source'] = 'brand_website'
        all_keywords.extend(brand_keywords)
        competitor_keywords = self.scrape_keywords(self.config['competitor_website'], country)
        for kw in competitor_keywords:
            kw['source'] = 'competitor_website'
        all_keywords.extend(competitor_keywords)
        return all_keywords

    def save_to_csv(self, keywords_data, filename=None):
        if not filename:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"kw_{timestamp}.csv"
        output_dir = os.getenv("SEM_OUTPUT_DIR")
        if output_dir:
            os.makedirs(output_dir, exist_ok=True)
            filename = os.path.join(output_dir, os.path.basename(filename))
        if keywords_data:
            df = pd.DataFrame(keywords_data)
            try:
                top_n = int(os.getenv("SEM_TOP_N", "10"))
            except Exception:
                top_n = 10
            if 'source' in df.columns and 'search_volume' in df.columns:
                df = (
                    df.sort_values(['source', 'search_volume'], ascending=[True, False])
                      .groupby('source', group_keys=False)
                      .head(top_n)
                )
            else:
                df = df.head(top_n)
            df.to_csv(filename, index=False)
            return filename
        return None

    def run_scraping(self):
        keywords_data = self.scrape_both_websites()
        if keywords_data:
            return self.save_to_csv(keywords_data)
        return None


def main():
    try:
        scraper = WordStreamScraper()
        result_file = scraper.run_scraping()
        if not result_file:
            return 1
    except Exception:
        return 1
    return 0

if __name__ == "__main__":
    exit(main()) 