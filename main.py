from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
import pandas as pd
import time
import logging
from concurrent.futures import ThreadPoolExecutor
import threading

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('scraper.log'),
        logging.StreamHandler()
    ]
)

class MuscleWikiScraper:
    def __init__(self):
        self.thread_local = threading.local()
        
    def get_driver(self):
        """Create or get thread-local driver instance"""
        if not hasattr(self.thread_local, "driver"):
            chrome_options = Options()
            
            # Headless mode and other configurations
            chrome_options.add_argument('--headless')
            chrome_options.add_argument('--window-size=1920x1080')
            chrome_options.add_argument('--disable-gpu')
            chrome_options.add_argument('--no-sandbox')
            chrome_options.add_argument('--disable-dev-shm-usage')
            chrome_options.add_argument('--ignore-certificate-errors')
            chrome_options.add_argument('--disable-extensions')
            chrome_options.add_argument('--disable-notifications')
            chrome_options.add_argument("--enable-unsafe-swiftshader")
            chrome_options.add_argument("--disable-gpu")
            chrome_options.add_argument("--log-level=3")

            chrome_options.page_load_strategy = 'eager'
            
            service = Service()
            self.thread_local.driver = webdriver.Chrome(service=service, options=chrome_options)
            self.thread_local.wait = WebDriverWait(self.thread_local.driver, 30)
            
        return self.thread_local.driver

    def close_driver(self):
        """Close thread-local driver instance"""
        if hasattr(self.thread_local, "driver"):
            self.thread_local.driver.quit()
            del self.thread_local.driver

    def wait_for_element(self, driver, by, value, timeout=60):
        """Wait for element with error handling"""
        try:
            element = WebDriverWait(driver, timeout,poll_frequency=0.5).until(
                EC.presence_of_element_located((by, value))
            )
            return element
        except TimeoutException:
            logging.error(f"Timeout waiting for element: {value}")
            return None
        except Exception as e:
            logging.error(f"Error finding element {value}: {str(e)}")
            return None
        
    def close_ad_popup(self, driver):
        """Try to close the ad popup if it appears."""
        try:
            # Cari elemen tombol tutup iklan dan klik jika ditemukan
            close_button = driver.find_element(By.XPATH, "/html/body/div[2]/div/div/div/div[2]/div/div/div[1]/button")
            close_button.click()
            logging.info("Pop-up ad closed successfully.")
            time.sleep(1)  # Beri waktu sejenak untuk pop-up benar-benar menghilang
        except NoSuchElementException:
            logging.info("No pop-up ad found.")
        except Exception as e:
            logging.error(f"Error closing pop-up ad: {str(e)}")

    def scrape_data(self, url, retries=3):
        """Scrape data from a single URL with retries in case of connection issues"""
        driver = self.get_driver()
        attempt = 0
        while attempt < retries:
            try:
                driver.get(url)
                time.sleep(3)
                self.close_ad_popup(driver)
                self.wait_for_element(driver, By.TAG_NAME, "body")

                # Proses scraping
                a_element = self.wait_for_element(driver, By.XPATH, "/html/body/div[1]/div/div[1]/div[2]/div[3]/main/div[2]/div[1]/div[1]/div[2]/div[1]/div[1]/video/source")
                b_element = self.wait_for_element(driver, By.XPATH, "/html/body/div[1]/div/div[1]/div[2]/div[3]/main/div[2]/div[1]/div[1]/div[2]/div[1]/div[2]/video/source")

                a_value = a_element.get_attribute("src") if a_element else "N/A"
                b_value = b_element.get_attribute("src") if b_element else "N/A"

                return {'a': a_value, 'b': b_value}
            
            except (ConnectionResetError, TimeoutException) as e:
                logging.warning(f"Connection error: {e}. Retrying {attempt + 1}/{retries}")
                attempt += 1
                time.sleep(3)  # Tunggu beberapa saat sebelum mengulangi
        
        logging.error(f"Failed to scrape {url} after {retries} retries sebanyak error")
        return {'a': 'ERROR', 'b': 'ERROR'}


    def process_pair(self, male_url, female_url, index, total_pairs):
        """Process a pair of male and female URLs and update progress"""
        male_data = self.scrape_data(male_url)
        female_data = self.scrape_data(female_url)
        
        # Calculate and log the progress
        progress = ((index + 1) / total_pairs) * 100
        logging.info(f"Progress: {progress:.2f}% - Processed index {index}")
        
        return {
            'index': index,
            'male_a': male_data['a'],
            'male_b': male_data['b'],
            'female_a': female_data['a'],
            'female_b': female_data['b']
        }

    def run_scraper(self):
        """Main method to run the scraper"""
        try:
            # Read the original CSV
            df = pd.read_csv("readme.csv")
            male_links = df['link_male'].tolist()
            female_links = df['link_female'].tolist()
            
            # Limit the number of links to scrape
            #male_links = male_link[0:10]
            #female_links = female_link[0:10]
            
            # Create pairs of URLs with their index
            url_pairs = list(enumerate(zip(male_links, female_links)))
            total_pairs = len(url_pairs)  # Total number of URL pairs
            
            # Initialize result storage
            results = []
            
            # Process URL pairs in parallel
            with ThreadPoolExecutor(max_workers=1) as executor:
                future_to_pair = {
                    executor.submit(self.process_pair, male_url, female_url, index, total_pairs): (index, male_url, female_url)
                    for index, (male_url, female_url) in url_pairs
                }
                
                for future in future_to_pair:
                    result = future.result()
                    results.append(result)
            
            # Sort results by index
            results.sort(key=lambda x: x['index'])
            
            # Ensure results match the number of links processed
            if len(results) == len(url_pairs):  # Change this to match the number of processed pairs
                for result in results:
                    df.at[result['index'], 'male_a'] = result['male_a']
                    df.at[result['index'], 'male_b'] = result['male_b']
                    df.at[result['index'], 'female_a'] = result['female_a']
                    df.at[result['index'], 'female_b'] = result['female_b']
                
                # Save the updated DataFrame
                df.to_csv("hasil.csv", index=False)
                logging.info("Scraping completed and results saved to resultstest.csv")
            else:
                logging.error("Results do not match the number of links processed")
                
        except Exception as e:
            logging.error(f"Error in run_scraper: {str(e)}")
        finally:
            # Clean up drivers for all threads
            self.close_driver()

if __name__ == "__main__":
    try:
        scraper = MuscleWikiScraper()
        scraper.run_scraper()
    except Exception as e:
        logging.error(f"Script failed: {str(e)}")
