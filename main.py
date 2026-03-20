from concurrent.futures import ProcessPoolExecutor, as_completed
from tqdm import tqdm
from src.utils import get_list_of_urls, parallel_function, integrate_file, group_files


if __name__ == '__main__':

    url_list: list = get_list_of_urls(2000, 1, 2026, 2)

    with ProcessPoolExecutor() as executor:
        futures = [executor.submit(parallel_function, url) for url in url_list]

        results = []

        for future in tqdm(as_completed(futures), total=len(futures)):
            results.append(future.result())

        grouped_paths = group_files(results)

        new_futures = [executor.submit(integrate_file, pathfiles) for pathfiles in grouped_paths.values()]

        for future in tqdm(as_completed(new_futures), total=len(new_futures)):
            print(f"Archivo creado: {future.result()}")

