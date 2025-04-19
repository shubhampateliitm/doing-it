"""
Configuration settings for the scrape_sentiments project.
"""

# Example configuration
API_KEY = "your_api_key_here"
BASE_URL = "https://example.com/api"

# Added Selenium WebDriver configuration options to the config file
SELENIUM_OPTIONS = {
    "headless": "--headless=new",
    "disable_gpu": "--disable-gpu",
    "window_size": "--window-size=1920,1080",
    "lang": "--lang=en-US",
    "disable_blink_features": "--disable-blink-features=AutomationControlled",
    "user_agent": "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/91.0.4472.124 Safari/537.36"
}

# Add search_terms to the config.py file
search_terms = ["hdfc", "tata motors"]
