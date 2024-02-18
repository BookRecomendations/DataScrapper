import math
import random
import time
import timeit
from typing import List, Tuple, Optional, Any
from datetime import timedelta
import requests
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor
import threading
import csv
import logging
import argparse

# Configure logging to save to a file and print to console
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(threadName)s - %(levelname)s - %(message)s',
                    handlers=[
                        logging.FileHandler('scraping.log', 'a'),
                        logging.StreamHandler()
                    ])
lock = threading.Lock()
lock_count = threading.Lock()
lock_error = threading.Lock()

FILE_NAME = 'new_descriptions.csv'
ERROR_FILE_NAME = 'scraping_errors.csv'
MAX_WORKERS = 128
CHUNKS = 100 * MAX_WORKERS

scraped_count = 0
start_time = timeit.default_timer()
total_urls = 0  # This will be set after reading the CSV

# Random headers to mimic a real browser
headers_list: List[dict[str, str]] = [
    {
        'User-Agent':
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'},
    {
        'User-Agent':
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0.3 Safari/605.1.15'},
    {
        'User-Agent':
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'},
    {
        'User-Agent':
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36'},
    {
        'User-Agent':
            'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/103.0.5060.53 Safari/537.36'},
    {
        'User-Agent':
            'Mozilla/5.0 (Windows NT 10.0; Windows; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/103.0.5060.114 Safari/537.36'},
]


def scrape_urls(data: List[Tuple[int, str]]) -> str:
    """
   Function to scrape URLs for book descriptions.

   Args:
       data (List[Tuple[int, str]]): List of tuples containing book ID and URL.

   Returns:
       str: Message indicating completion of scraping.
   """
    global scraped_count

    results: List[Tuple[int, Optional[str]]] = []
    for row in data:
        book_id, url = row
        try:
            headers = random.choice(headers_list)
            time.sleep(random.uniform(1, 4))
            response = requests.get(url, headers=headers)
            response.raise_for_status()

            soup = BeautifulSoup(response.text, 'html.parser')
            description_div = soup.find('div', class_='DetailsLayoutRightParagraph')
            if description_div:
                description = description_div.text.strip()
                logging.info(f"Scraped description for ID {book_id} from {url}")
                # If description is too short, set it to None
                if len(description) < 10:
                    description = None
                results.append((book_id, description))
            else:
                logging.warning(f"No description found for ID {book_id} at {url}")
                results.append((book_id, None))
        except Exception as e:
            logging.error(f"Error scraping URL {url} for ID {book_id}: {e}")
            error_message = str(e)
            results.append((book_id, None))
            log_error_to_csv(book_id, url, error_message)
        finally:
            with lock_count:
                scraped_count += 1
                if scraped_count % 10 == 0:
                    elapsed_time = timeit.default_timer() - start_time
                    remaining_urls = total_urls - scraped_count
                    avg_time_per_url = elapsed_time / scraped_count if scraped_count else float('inf')
                    approx_time_left = avg_time_per_url * remaining_urls

                    # Convert seconds into a timedelta object, then format as HH:MM:SS
                    approx_time_left_formatted = str(timedelta(seconds=int(approx_time_left)))

                    logging.info(
                        f"Scraped {scraped_count}/{total_urls}. Approx. time left: {approx_time_left_formatted} (HH:MM:SS)")
                if scraped_count == total_urls:
                    logging.info(f"All URLs scraped")
    save_to_csv(results)
    return "Scraping complete"


def save_to_csv(data: List[Tuple[int, str]], file_path: str = FILE_NAME) -> None:
    """
    Function to save scraped data to a CSV file.

    Args:
        data (List[Tuple[int, str]]): List of tuples containing book ID and description.
        file_path (str, optional): Path to the CSV file. Defaults to FILE_NAME.
    """
    try:
        with lock:
            with open(file_path, 'a', newline='', encoding='utf-8') as file:
                writer = csv.writer(file)
                for row in data:
                    writer.writerow(row)
                    logging.info(f"Data for ID {row[0]} saved to CSV")
    except Exception as e:
        ids = [row[0] for row in data]
        logging.error(f"Error saving data with ID's {ids} to CSV: {e}")


def log_error_to_csv(book_id: int, url: str, error_message: str, file_path: str = ERROR_FILE_NAME) -> None:
    """
    Function to log errors to a CSV file during scraping.

    Args:
        book_id (int): The ID of the book that encountered an error.
        url (str): The URL that was being scraped when the error occurred.
        error_message (str): A description of the error.
        file_path (str, optional): Path to the error log CSV file. Defaults to ERROR_FILE_NAME.
    """
    with lock_error:
        with open(file_path, 'a', newline='', encoding='utf-8') as file:
            writer = csv.writer(file)
            writer.writerow([book_id, url, error_message])
            logging.info(f"Logged error for ID {book_id} to {file_path}")


def chunk_data(data: List[Any], n: int) -> List[List[Any]]:
    """
    Function to split data into n chunks.

    Args:
        data (List[Any]): List of data to be split.
        n (int): Number of chunks.

    Returns:
        List[List[Any]]: List of chunks.
    """
    k, m = divmod(len(data), n)
    return [data[i * k + min(i, m):(i + 1) * k + min(i + 1, m)] for i in range(n)]


def read_id_url_pairs_from_csv(file_path: str = 'bad_descriptions.csv') -> List[List[Tuple[int, str]]]:
    """
    Function to read book ID and URL pairs from a CSV file.

    Args:
        file_path (str, optional): Path to the CSV file. Defaults to 'bad_descriptions.csv'.

    Returns:
        List[List[Tuple[int, str]]]: List of book ID and URL pairs.
    """
    global total_urls

    id_url_pairs = []
    with open(file_path, 'r', newline='', encoding='utf-8') as file:
        reader = csv.reader(file)
        for row in reader:
            try:
                id_url_pairs.append((int(row[1]), row[2]))
            except ValueError:
                logging.error(f"Error reading row {row}: ID must be an integer")
            except IndexError:
                logging.error(f"Error reading row {row}: Row must have at least two columns")

    logging.info(f"Read {len(id_url_pairs)} ID-URL pairs from CSV")
    total_urls = len(id_url_pairs)

    return chunk_data(id_url_pairs, int(math.sqrt(len(id_url_pairs) * MAX_WORKERS) * 2))


def get_args():
    parser = argparse.ArgumentParser(description="Scrape book descriptions from URLs.")
    parser.add_argument('file_path', type=str, help='Path to the CSV file containing book ID and URL pairs.')
    parser.add_argument('--max_workers', type=int, default=16,
                        help='Number of workers for ThreadPoolExecutor. Defaults to 16.')
    return parser.parse_args()


def main(file_path: str, max_workers: int = 16) -> None:
    """
    Main function to initiate scraping.

    Args:
        file_path (str): Path to the CSV file containing book ID and URL pairs.
        max_workers (int, optional): Number of workers for ThreadPoolExecutor. Defaults to 16.
    """
    id_url_pairs = read_id_url_pairs_from_csv(file_path)

    try:
        with lock:
            with open(FILE_NAME, 'a', newline='', encoding='utf-8') as file:
                if file.tell() == 0:  # Check if file is empty to write headers
                    csv.writer(file).writerow(['ID', 'Description'])

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            executor.map(scrape_urls, id_url_pairs)
            logging.info("Scraping initiated")
    except Exception as e:
        logging.error(f"Error in main function: {e}")


if __name__ == "__main__":
    args = get_args()
    file_path = args.file_path if args.file_path else 'bad_descriptions_138.csv'
    max_workers = args.max_workers

    logging.info("Starting scraping")
    start = timeit.default_timer()
    main(file_path, max_workers=max_workers)
    end = timeit.default_timer()
    logging.info(f"Scraping complete. Time taken:  Time taken: {timedelta(seconds=int(end - start))}")
