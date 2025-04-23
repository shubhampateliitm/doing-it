"""
Web scraping logic for the scrape_sentiments project.
"""
from abc import ABC, abstractmethod
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.firefox.options import Options
from selenium.common.exceptions import NoSuchElementException, TimeoutException, WebDriverException
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from datetime import datetime
from .config import SELENIUM_OPTIONS
import pandas as pd
import time
import random
from retrying import retry
import logging
import traceback

from .sentiment_analysis import analyze_sentiment

# Configure logging
log_level = logging.INFO  # Default log level
logging.basicConfig(
    level=log_level,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

class BaseScraper(ABC):
    """Abstract base class for web scrapers."""

    def __init__(self, date, search_terms=None):
        self.date = date
        self.search_terms = search_terms
        self.driver = None

    def setup_driver(self):
        """Set up the Selenium WebDriver with options. If Chrome is not available, attempt to set it up."""
        try:
            options = Options()
            options.add_argument(SELENIUM_OPTIONS["headless"])
            options.add_argument(SELENIUM_OPTIONS["disable_gpu"])
            options.add_argument(SELENIUM_OPTIONS["window_size"])
            options.add_argument(SELENIUM_OPTIONS["lang"])
            options.add_argument(SELENIUM_OPTIONS["disable_blink_features"])
            options.add_argument(SELENIUM_OPTIONS["user_agent"])
            self.driver = webdriver.Remote(
                    command_executor='http://firefox:4444/wd/hub',
                    options=options
            )
        except Exception as e:
            logging.error("Firefox WebDriver setup failed: %s", str(e))
            logging.info("Attempting to set up Firefox WebDriver.")

    def close_driver(self):
        """Close the Selenium WebDriver."""
        if self.driver:
            self.driver.quit()

    @abstractmethod
    def scrape(self):
        """Abstract method to scrape data. Must be implemented by subclasses."""
        pass

class YourStoryScraper(BaseScraper):
    """Scraper for YourStory website."""

    def __init__(self, date, search_terms):
        super().__init__(date, search_terms)
        logging.info("Initialized YourStoryScraper with date: %s and search terms: %s", date, search_terms)

    @retry(stop_max_attempt_number=3, wait_random_min=1000, wait_random_max=3000)
    def fetch_article_urls(self, page_source, target_date):
        logging.info("Fetching article URLs for target date: %s", target_date)
        time.sleep(random.uniform(1, 3))
        soup = BeautifulSoup(page_source, "html.parser")
        story_div = soup.find("div", class_="storyItem")

        if not story_div:
            logging.warning("No story items found on the page.")
            return []

        nested_divs = story_div.find_all(class_="sc-68e2f78-2 bLuPDa")
        base_url = self.driver.current_url
        final_urls_with_dates = []

        for div in nested_divs:
            li_items = div.find_all("li", class_="sc-c9f6afaa-0")
            for li in li_items:
                a_tag = li.find("a")
                date_of_article = li.find("span", class_="sc-36431a7-0 dpmmXH").get_text(strip=True)
                if a_tag and a_tag.get("href"):
                    relative_url = a_tag["href"]
                    absolute_url = urljoin(base_url, relative_url)
                    final_urls_with_dates.append((date_of_article, absolute_url))

        logging.info("Fetched %d article URLs for target date: %s", len(final_urls_with_dates), target_date)

        target_date_obj = datetime.strptime(target_date, "%Y-%m-%d")
        filtered_urls_with_dates = [
            (date, url) for (date, url) in final_urls_with_dates
            if datetime.strptime(date, "%m/%d/%Y") >= target_date_obj
        ]

        if not filtered_urls_with_dates:
            logging.warning("No articles found for the given date: %s", target_date)
            return []

        # Limit to no more than 5 URLs
        return filtered_urls_with_dates[:5]

    @retry(stop_max_attempt_number=3, wait_random_min=1000, wait_random_max=3000)
    def fetch_article_content(self, article_url):
        logging.info("Fetching content for article URL: %s", article_url)
        try:
            self.setup_driver()
            time.sleep(random.uniform(1, 3))  # Random delay to avoid bot detection
            self.driver.get(article_url)

            # Wait for the article container to load
            WebDriverWait(self.driver, 120).until(
                EC.visibility_of_element_located((By.ID, "article_container"))
            )

            # Scrape article details
            html = self.driver.page_source
            soup = BeautifulSoup(html, "html.parser")
            article = soup.find("div", id="article_container").get_text(separator=" ", strip=True)
            header = soup.find("h1").get_text(separator=" ", strip=True)
            tagline = soup.find("h2").get_text(separator=" ", strip=True)

            logging.info("Successfully fetched content for article URL: %s", article_url)
            return article, header, tagline
        except Exception as e:
            logging.error("Error fetching content for article URL: %s. Error: %s", article_url, str(e))
            logging.error("Page source at the time of error: %s", self.driver.page_source[:100])
            logging.info("Re-setting up Selenium WebDriver due to failure.")
            self.close_driver()
            self.setup_driver()
            raise
        finally:
            self.close_driver()

    def get_scraped_results(self, article_urls):
        """Fetch and return scraped results for a list of article URLs as a pandas DataFrame, including sentiment scores."""

        results = []
        for article_date, article_url in article_urls:
            article, header, tagline = self.fetch_article_content(article_url)
            sentiment_score = analyze_sentiment(article)
            results.append({
                "date": article_date,
                "url": article_url,
                "article": article,
                "header": header,
                "tagline": tagline,
                "sentiment_score": sentiment_score,
            })
        # Convert results to a pandas DataFrame
        return pd.DataFrame(results)

    def scrape(self):
        logging.info("Starting scraping process.")
        all_results = []
        try:
            logging.info("WebDriver setup completed.")
            for search_term in self.search_terms:
                try:
                    self.setup_driver()
                    logging.info("Scraping articles for search term: %s", search_term)
                    base_url = f"https://yourstory.com/search?q={search_term}&page=1"
                    self.driver.get(base_url)

                # Wait for the element with class 'storyItem' to be visible
                    WebDriverWait(self.driver, 120).until(
                        EC.visibility_of_element_located((By.CLASS_NAME, "storyItem"))
                    )
                    page_source = self.driver.page_source
                    article_urls = self.fetch_article_urls(page_source, self.date)
                except:
                    raise("fail to fetch")
                finally:
                    self.close_driver()
                # Use a separate function to get scraped results
                results = self.get_scraped_results(article_urls)
                all_results.append(results)
        except TimeoutException:
            logging.error("Timeout while waiting for the page to load.")
            logging.error("Stack trace:")
            logging.error(traceback.format_exc())
        except Exception as e:
            logging.error("An error occurred during scraping: %s", str(e))
            logging.error("Stack trace:")
            logging.error(traceback.format_exc())
            
        # Combine all results into a single DataFrame
        if all_results:
            logging.info("Combining all scraped results into a single DataFrame.")
            return pd.concat(all_results, ignore_index=True)
        else:
            logging.warning("No results to combine. Returning an empty DataFrame.")
            return pd.DataFrame()

class FinshotsScraper(BaseScraper):
    """Scraper for Finshots website."""

    def __init__(self, date, search_terms):
        super().__init__(date, search_terms)
        self.base_url = "https://finshots.in"

    def scrape(self):
        results = []
        for search_term in self.search_terms:
            try:
                self.setup_driver()
                self.driver.get(self.base_url)
                self.search_for_keyword(search_term)
                time.sleep(random.uniform(5, 8))
                results.append(self.walk_through_search_results(self.date, search_term))
            except TimeoutException:
                logging.error("Timeout while waiting for the page to load.")
                logging.error("Stack trace:")
                logging.error(traceback.format_exc())
            except Exception as e:
                logging.error("An error occurred during scraping: %s", str(e))
                logging.error("Stack trace:")
                logging.error(traceback.format_exc())
            finally:
                self.close_driver()
        # Combine all results into a single DataFrame
        print(results)
        if results:
            try:
                logging.info("Combining all scraped results into a single DataFrame.")
                return pd.concat(results, ignore_index=True)
            except Exception as e:
                logging.error("Error while combining results: %s", str(e))
                logging.error(traceback.format_exc())
        else:
            logging.warning("No results to combine. Returning an empty DataFrame.")
            return pd.DataFrame()

    def click_on_search_box(self):
        try:
            button = self.driver.find_element(By.CSS_SELECTOR, "button.gh-search")
            self.driver.execute_script("arguments[0].click();", button)
            time.sleep(random.uniform(5, 8))  # Random sleep to mimic human interaction
            logging.info("Clicked on the search box successfully.")
        except NoSuchElementException:
            logging.error("Search button not found.")
        except WebDriverException as e:
            logging.error(f"Error while clicking on the search box: {e}")

    def put_text_in_search_box(self, text):
        try:
            iframe = self.driver.find_element(By.CSS_SELECTOR, '#sodo-search-root iframe')
            self.driver.switch_to.frame(iframe)
            logging.info("Switched to iframe successfully.")
            search_input = self.driver.find_element(By.CSS_SELECTOR, 'input[placeholder="Search posts and tags"]')
            search_input.send_keys(text)
            logging.info(f"Entered text '{text}' in the search box successfully.")
        except NoSuchElementException:
            logging.error("Search input box or iframe not found.")
        except WebDriverException as e:
            logging.error(f"Error while interacting with the search box: {e}")
        finally:
            self.driver.switch_to.default_content()  # Ensure we switch back to the default content

    def search_for_keyword(self, keyword):
        try:
            self.click_on_search_box()
            self.put_text_in_search_box(keyword)
            logging.info(f"Search for keyword '{keyword}' completed successfully.")
        except Exception as e:
            logging.error(f"An error occurred during the search process: {e}")

    def walk_through_search_results(self,date, keyword):
        try:
            iframe = self.driver.find_element(By.CSS_SELECTOR, '#sodo-search-root iframe')
            self.driver.switch_to.frame(iframe)
            total_post = len(self.driver.find_elements(By.CSS_SELECTOR, "div.cursor-pointer")[:5])
            self.driver.switch_to.default_content()
            results = []

            for i in range(total_post):
                self.driver.get(self.base_url)
                time.sleep(random.uniform(5, 8))
                self.search_for_keyword(keyword)
                time.sleep(random.uniform(5, 8))
                iframe = self.driver.find_element(By.CSS_SELECTOR, '#sodo-search-root iframe')
                self.driver.switch_to.frame(iframe)
                posts = self.driver.find_elements(By.CSS_SELECTOR, "div.cursor-pointer")[:5]
                posts[i].click()
                time.sleep(random.uniform(2, 5)) 
                result = self.get_article_content()
                article_date = None
                if result:
                    article_date, url, article, title = result
                    if article_date >= date:
                        sentiment_score = analyze_sentiment(article)
                        results.append({
                        "date": article_date,
                        "url": url,
                        "article": article,
                        "header": title,
                        "tagline": "",
                        "sentiment_score": sentiment_score,
                        })
                time.sleep(random.uniform(2, 5))
                if article_date and article_date < date:
                    break
                
        except Exception as e:
            logging.error(f"Error during walking through search results: {e}")
            logging.error("Stack trace:")
            logging.error(traceback.format_exc())
        finally:
            self.driver.switch_to.default_content()
            return pd.DataFrame(results)
        

    def get_article_content(self):
        try:
            url = self.driver.current_url
            self.driver.get(url)
            time.sleep(random.uniform(5, 8))  # Random sleep to mimic human interaction
            logging.info(f"Current URL: {url}")
            # Wait for the article content to load
            soup = BeautifulSoup(self.driver.page_source, 'html.parser')
            date = soup.find("time", class_="post-full-meta-date")["datetime"]
            title = soup.find("h1", class_="article-title").get_text(separator=" ", strip=True)
            article = soup.find("section", class_="gh-content").get_text(separator=" ", strip=True)
            print("Extracted article content successfully")
            return date, url, article, title
        except Exception as e:
            logging.error(f"Error while extracting article content: {e}")