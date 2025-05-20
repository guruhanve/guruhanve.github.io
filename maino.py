import requests
from bs4 import BeautifulSoup
import time
import os

# Base URL and file path
base_url = "https://moviesmod.email/"
output_file = "./urlmovie.txt"  # Adjust to /workspaces/guruhanve.github.io/urlmovie.txt if needed

# Headers to mimic a browser
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
}

# Counter for URLs found
url_count = 0

# Ensure the output directory exists
try:
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
except OSError as e:
    print(f"Error creating directory for {output_file}: {e}")
    exit(1)

# Function to append a single URL to the file
def save_url(url):
    global url_count
    try:
        with open(output_file, 'a', encoding='utf-8') as f:
            f.write(url + '\n')
        url_count += 1
        print(f"Saved URL {url_count}: {url}")
    except PermissionError:
        print(f"Permission denied: Unable to write to {output_file}. Please check file permissions.")
        return False
    except OSError as e:
        print(f"Error writing to {output_file}: {e}")
        return False
    return True

# Function to scrape URLs from a single page
def scrape_page(url):
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()  # Raise an exception for bad status codes
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Find all <a> tags with href attributes
        links = soup.find_all('a', href=True)
        for link in links:
            href = link['href']
            # Check if the URL matches the pattern for movie download links
            if "https://moviesmod.email/download-" in href:
                if not save_url(href):  # Save URL immediately
                    return None  # Stop if file writing fails
        
        # Find the "Next" button
        next_button = soup.find('a', class_='next page-numbers')
        return next_button['href'] if next_button else None
    except requests.RequestException as e:
        print(f"Error fetching {url}: {e}")
        return None

# Main scraping loop
current_url = base_url
page_count = 1

while current_url and page_count <= 921:  # Limit to 921 pages
    print(f"Scraping page {page_count}: {current_url}")
    next_url = scrape_page(current_url)
    if not next_url:
        print("No 'Next' button found or file error, stopping.")
        break
    current_url = next_url
    page_count += 1
    time.sleep(1)  # Be polite, avoid overwhelming the server

print(f"Scraping completed. Total URLs saved: {url_count}")
if url_count > 0:
    print(f"URLs saved to {output_file}")
else:
    print("No URLs were found or saved.")