from concurrent.futures import ProcessPoolExecutor, as_completed
from tqdm import tqdm
import time

def get_url_list():
    #TODO: generar listado de archivos .nc a descargar desde la página de la NOA.
    #TODO: retorna listado de archivos (url: str)
    return list(range(50))

def parallel_function(url: str):
    def download_file(url: str):
        #TODO: generar descarga del archivo en url y guardar en ./data
        #TODO: retornar ruta del archivo descargado ej: ./data/sst2026-01.nc
        return "path.nc"

    def netcdf2cpt(path: str):
        #TODO: leer archivo netCDF guardado en path
        #TODO: leer variable sst y dimensiones Lat. Lon.
        #TODO: convertir en formato .tsv como solicitado por CPT
        #TODO: retornar ruta (path) del archivo .tsv generado y guardado, ej: ./data/sst2026-01-cpt.tsv
        return "path.tsv"

    time.sleep(5)
    return "path.tsv"

def integrate(paths: list[str]):
    #TODO: integrar todos los archivos .tsv generados en un solo archivo .tsv según lo solicitado por CPT
    #TODO: retornar ubicación del archivo consolidado
    return "file-CPT.tsv"


if __name__ == '__main__':
    
    url_list: list = get_url_list()

    with ProcessPoolExecutor() as executor:
        futures = [executor.submit(parallel_function, url) for url in get_url_list()]

        results = []

        for future in tqdm(as_completed(futures), total=len(futures)):
            results.append(future.result())

    
    integrate(results)