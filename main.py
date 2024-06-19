import requests
import io
from concurrent.futures import ThreadPoolExecutor
import psycopg2
from psycopg2.extras import execute_batch
import json
import logging
import time

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

batch_size = None


def ingest_qradar_data(api_token, search_id, redshift_conn_params):
    base_url = "https://<your_qradar_host>/api/ariel/searches"
    headers = {"SEC": api_token}

    def fetch_batch(offset, batch_size):
        retries = 3  # Number of retry attempts
        while retries > 0:
            try:
                url = (
                    f"{base_url}/{search_id}/results?offset={offset}&range={batch_size}"
                )
                response = requests.get(
                    url, headers=headers, stream=True, timeout=300
                )  # Add timeout
                response.raise_for_status()
                logging.info(f"Fetched batch from offset: {offset}")
                batch_size = response.headers.get("record_count")
                for chunk in response.iter_content(chunk_size=1024 * 1024):
                    yield chunk
                break  # Exit the loop if successful
            except requests.exceptions.RequestException as e:
                logging.error(f"Error fetching batch: {e}. Retrying...")
                retries -= 1
                time.sleep(5)  # Wait before retrying
        else:
            logging.critical(f"Failed to fetch batch after {retries} retries.")
            raise Exception("Failed to fetch batch from QRadar")

    def insert_batch(batch):
        with psycopg2.connect(**redshift_conn_params) as conn:
            with conn.cursor() as cur:
                try:
                    insert_query = """
                        COPY your_redshift_table (column1, column2, ...)
                        FROM STDIN
                        JSON 'auto'
                        TIMEFORMAT 'auto'
                    """
                    cur.copy_expert(insert_query, io.BytesIO(batch))
                    logging.info("Batch inserted successfully")
                except psycopg2.Error as e:
                    logging.error(f"Error inserting batch: {e}")
                    raise

    num_batches = 10

    with ThreadPoolExecutor(max_workers=num_batches) as executor:
        futures = [
            executor.submit(fetch_batch, i * batch_size, batch_size)
            for i in range(num_batches)
        ]

        for future in futures:
            for chunk in future.result():
                insert_batch(chunk)


if __name__ == "__main__":
    api_token = "YOUR_QRADAR_API_TOKEN"
    search_id = "YOUR_SEARCH_ID"
    redshift_conn_params = {
        # ... your Redshift connection parameters
    }

    ingest_qradar_data(api_token, search_id, redshift_conn_params)
