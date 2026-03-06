from concurrent.futures import ProcessPoolExecutor, as_completed
from tqdm import tqdm
import time
from src.utils import get_list_of_urls, parallel_function, integrate_file


if __name__ == '__main__':

    url_list: list = get_list_of_urls(2024, 1, 2026, 2)

    with ProcessPoolExecutor() as executor:
        futures = [executor.submit(parallel_function, url) for url in url_list]

        results = []

        for future in tqdm(as_completed(futures), total=len(futures)):
            results.append(future.result())

    
    integrate_file(results)