from concurrent.futures import ProcessPoolExecutor, as_completed
from tqdm import tqdm
import time

def get_url_list():
    return list(range(50))

def parallel_function(url: str):
    def download_file(url: str):
        return "path.nc"

    def netcdf2cpt(path: str):
        return "path.tsv"

    time.sleep(5)
    return "path.tsv"

def integrate(paths: list[str]):
    pass


if __name__ == '__main__':
    
    url_list: list = get_url_list()

    with ProcessPoolExecutor() as executor:
        futures = [executor.submit(parallel_function, url) for url in get_url_list()]

        results = []

        for future in tqdm(as_completed(futures), total=len(futures)):
            results.append(future.result())

    
    integrate(results)