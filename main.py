import asyncio
import os
from queue import Queue
from datetime import datetime
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError, Error as PlaywrightError
from colorama import Fore, Style, init
from tabulate import tabulate

# Initialize colorama for colored output
init(autoreset=True)

# Bright Data Browser API credentials
AUTH = 'brd-customer-hl_dfdbd0b1-zone-scraping_browser1:m5ocs1sb5xkp'
SBR_WS_CDP = f'wss://{AUTH}@brd.superproxy.io:9222'
URL_FILE = '/workspaces/guruhanve.github.io/url.txt'  # File containing URLs

# Global log storage and lock
log_entries = []
log_lock = asyncio.Lock()
MAX_LOG_ENTRIES = 20  # Limit table rows

async def log_table(browser_id, cycle, step, status, details):
    """Add a log entry and print the table."""
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    # Color-code status
    if status == 'SUCCESS':
        status = f"{Fore.GREEN}{status}{Style.RESET_ALL}"
    elif status == 'ERROR':
        status = f"{Fore.RED}{status}{Style.RESET_ALL}"
    elif status == 'WARNING':
        status = f"{Fore.YELLOW}{status}{Style.RESET_ALL}"
    else:
        status = f"{Fore.CYAN}{status}{Style.RESET_ALL}"

    entry = [timestamp, f"Browser {browser_id}", cycle, step, status, details]

    async with log_lock:
        log_entries.append(entry)
        if len(log_entries) > MAX_LOG_ENTRIES:
            log_entries.pop(0)  # Remove oldest entry

        # Clear console and print table
        os.system('cls' if os.name == 'nt' else 'clear')
        print(tabulate(
            log_entries,
            headers=['Timestamp', 'Browser ID', 'Cycle', 'Step', 'Status', 'Details'],
            tablefmt='grid',
            stralign='left'
        ))

async def load_urls():
    """Load URLs from file."""
    try:
        with open(URL_FILE, 'r') as file:
            urls = [url.strip() for url in file.readlines() if url.strip()]
        if not urls:
            raise ValueError("No URLs found in the file.")
        return urls
    except FileNotFoundError:
        raise FileNotFoundError(f"File '{URL_FILE}' not found.")

async def process_url(instance_id, url, pw, url_queue, cycle_count):
    """Process a single URL with all steps. Return True on success, False on failure."""
    await log_table(instance_id, cycle_count, 0, 'INFO', f"Starting processing URL: {url}")

    # Connect to Bright Data Browser API
    browser = None
    try:
        await log_table(instance_id, cycle_count, 0, 'INFO', "Connecting to Browser API...")
        browser = await pw.chromium.connect_over_cdp(SBR_WS_CDP, timeout=60000)  # 60s timeout
        page = await browser.new_page()
        await log_table(instance_id, cycle_count, 0, 'SUCCESS', "Connected! Navigating to webpage")

        # Handle popups by closing new pages
        async def handle_new_page(new_page):
            try:
                await new_page.close()
                await log_table(instance_id, cycle_count, 0, 'WARNING', "Closed a popup window")
            except PlaywrightError:
                pass  # Ignore errors during popup closure
        browser.on("page", handle_new_page)

        # Navigate to the URL
        try:
            await page.goto(url, timeout=60000)  # 60s timeout
            await page.wait_for_load_state('load', timeout=60000)
            await log_table(instance_id, cycle_count, 1, 'SUCCESS', "Page loaded successfully")
        except PlaywrightTimeoutError:
            await log_table(instance_id, cycle_count, 1, 'ERROR', "Timeout during page navigation")
            return False
        except PlaywrightError as e:
            await log_table(instance_id, cycle_count, 1, 'ERROR', f"Navigation error: {str(e)}")
            return False

        # Define the steps with selectors and navigation flags
        steps = [
            {"selector": "#robot", "navigate": False, "wait_10ms": False, "desc": "Click 'I Am Not Robot'"},
            {"selector": "#rtgli1", "navigate": False, "wait_10ms": True, "desc": "Click 'Dual Tap Go Link'"},
            {"selector": "#robot2", "navigate": False, "wait_10ms": True, "desc": "Click 'Dual Tap Continue'"},
            {"selector": "#rtg-snp2", "navigate": True, "wait_10ms": True, "desc": "Click 'Open Continue..'"},
            {"selector": "#robotButton", "navigate": False, "wait_10ms": False, "desc": "Click 'Dual Tap Verify'"},
            {"selector": "#rtgli1", "navigate": False, "wait_10ms": True, "desc": "Click 'Dual Tap To Go To Link'"},
            {"selector": "#robotContinueButton", "navigate": False, "wait_10ms": True, "desc": "Click 'Dual Tap Continue'"},
            {"selector": "#rtg-snp2", "navigate": True, "wait_10ms": True, "desc": "Click 'Open-Continue'"},
            {"selector": "#robotButton", "navigate": False, "wait_10ms": False, "desc": "Click 'Dual Tap Verify'"},
            {"selector": "#rtgli1", "navigate": False, "wait_10ms": True, "desc": "Click 'Dual Tap To Go To Link'"},
            {"selector": "#robotContinueButton1", "navigate": False, "wait_10ms": True, "desc": "Click 'Dual Tap Continue'"},
            {"selector": "#open-continue-btn", "navigate": True, "wait_10ms": True, "desc": "Click 'Open-Continue'"},
            {"selector": ".btn.btn-lg.get-link", "navigate": False, "wait_10ms": True, "desc": "Click 'Get Link' (Human-like)"},
        ]

        # Execute each step
        for i, step in enumerate(steps, start=1):
            selector = step["selector"]
            navigate = step["navigate"]
            wait_10ms = step["wait_10ms"]
            desc = step["desc"]

            # Wait for the element
            await log_table(instance_id, cycle_count, i, 'INFO', f"Waiting for '{selector}' ({desc})")
            try:
                if wait_10ms:
                    await page.wait_for_function(
                        f'document.querySelector("{selector}") !== null',
                        timeout=30000,
                        polling=10
                    )
                else:
                    await page.wait_for_selector(selector, timeout=30000)
            except PlaywrightTimeoutError:
                await log_table(instance_id, cycle_count, i, 'ERROR', f"Timeout waiting for '{selector}'")
                return False
            except PlaywrightError as e:
                await log_table(instance_id, cycle_countFearlessly, i, 'ERROR', f"Element wait error: {str(e)}")
                return False

            # Perform the click
            await log_table(instance_id, cycle_count, i, 'INFO', f"Clicking '{selector}' ({desc})")
            try:
                if i == 13:  # Last step uses human-like click
                    if navigate:
                        async with page.expect_navigation(timeout=60000):
                            await page.click(selector, timeout=30000)
                    else:
                        await page.click(selector, timeout=30000)
                else:
                    # Other steps use JavaScript click
                    if navigate:
                        async with page.expect_navigation(timeout=60000):
                            await page.evaluate('selector => document.querySelector(selector).click()', selector)
                    else:
                        await page.evaluate('selector => document.querySelector(selector).click()', selector)
            except PlaywrightTimeoutError:
                await log_table(instance_id, cycle_count, i, 'ERROR', f"Timeout during click on '{selector}'")
                return False
            except PlaywrightError as e:
                await log_table(instance_id, cycle_count, i, 'ERROR', f"Click error: {str(e)}")
                return False
            except asyncio.CancelledError:
                await log_table(instance_id, cycle_count, i, 'ERROR', f"Operation cancelled during click on '{selector}'")
                return False

        await log_table(instance_id, cycle_count, 13, 'SUCCESS', f"All steps completed for URL: {url}")
        return True
    except PlaywrightError as e:
        await log_table(instance_id, cycle_count, 0, 'ERROR', f"Browser error: {str(e)}")
        return False
    except asyncio.CancelledError:
        await log_table(instance_id, cycle_count, 0, 'ERROR', "Operation cancelled")
        return False
    except Exception as e:
        await log_table(instance_id, cycle_count, 0, 'ERROR', f"Unexpected error processing URL {url}: {str(e)}")
        return False
    finally:
        if browser:
            try:
                await browser.close()
                await log_table(instance_id, cycle_count, 0, 'INFO', "Browser closed")
            except PlaywrightError:
                await log_table(instance_id, cycle_count, 0, 'WARNING', "Browser already closed")

async def worker(instance_id, url_queue, pw, cycle_count):
    """Worker function for each browser instance."""
    while True:
        try:
            url = url_queue.get_nowait()
            success = await process_url(instance_id, url, pw, url_queue, cycle_count)
            url_queue.task_done()
            if success:
                await log_table(instance_id, cycle_count, 0, 'WARNING', "Restarting with new URL after success")
            else:
                await log_table(instance_id, cycle_count, 0, 'WARNING', "Restarting with new URL due to error")
            continue  # Fetch a new URL
        except Queue.Empty:
            await log_table(instance_id, cycle_count, 0, 'INFO', "No more URLs to process in this cycle")
            break
        except Exception as e:
            await log_table(instance_id, cycle_count, 0, 'ERROR', f"Worker error: {str(e)}")
            # Do not call task_done() here to avoid queue imbalance

async def main():
    async with async_playwright() as playwright:
        cycle_count = 0
        while True:
            cycle_count += 1
            await log_table(0, cycle_count, 0, 'INFO', f"Starting Cycle {cycle_count}")

            # Load URLs
            try:
                urls = await load_urls()
                await log_table(0, cycle_count, 0, 'SUCCESS', f"Loaded {len(urls)} URLs")
            except Exception as e:
                await log_table(0, cycle_count, 0, 'ERROR', f"Failed to load URLs: {str(e)}")
                await asyncio.sleep(60)  # Wait before retrying
                continue

            # Create URL queue
            url_queue = Queue()
            for url in urls:
                url_queue.put(url)

            # Run 5 parallel workers
            tasks = [
                worker(i + 1, url_queue, playwright, cycle_count)
                for i in range(5)
            ]
            try:
                await asyncio.gather(*tasks)
            except asyncio.CancelledError:
                await log_table(0, cycle_count, 0, 'ERROR', "Cycle cancelled")
                continue

            await log_table(0, cycle_count, 0, 'SUCCESS', f"Cycle {cycle_count} completed")
            await asyncio.sleep(1)  # Brief pause before restarting

if __name__ == '__main__':
    asyncio.run(main())