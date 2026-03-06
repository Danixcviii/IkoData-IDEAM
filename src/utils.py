import requests
from bs4 import BeautifulSoup
from typing import List, Tuple

import xarray as xr
import os
import re # librería de expresiones regulares

def get_list_of_urls(
    ano_inicio: int, mes_inicio: int,
    ano_fin: int, mes_fin: int,
    url_base: str = "https://www.ncei.noaa.gov/pub/data/cmb/ersst/v5/netcdf/"
) -> List[str]:
    """
    Obtiene los enlaces de los archivos ERSST v5 para un rango de fechas.

    Args:
        ano_inicio, mes_inicio: Fecha de inicio (año y mes, ej: 1854, 1).
        ano_fin, mes_fin: Fecha de fin (año y mes, ej: 1855, 12).
        url_base: URL del directorio con los archivos .nc.

    Returns:
        Lista de URLs completas de los archivos que cumplen el rango.

    Raises:
        Exception: Si hay problemas al obtener o parsear la página.
    """
    # 1. Obtener el contenido HTML del índice
    try:
        respuesta = requests.get(url_base, timeout=10)
        respuesta.raise_for_status()  # Lanza error si la petición falla
    except requests.exceptions.RequestException as e:
        raise Exception(f"Error al obtener el índice: {e}")

    # 2. Parsear el HTML y extraer los nombres de archivo .nc
    soup = BeautifulSoup(respuesta.text, 'html.parser')
    enlaces = []
    for link in soup.find_all('a'):
        href = link.get('href')
        if href and href.endswith('.nc') and href.startswith('ersst.v5.'):
            enlaces.append(href)

    # 3. Función para extraer año y mes del nombre del archivo
    def extraer_fecha(nombre_archivo: str) -> Tuple[int, int]:
        # El formato es "ersst.v5.YYYYMM.nc"
        parte_fecha = nombre_archivo.replace('ersst.v5.', '').replace('.nc', '')
        ano = int(parte_fecha[:4])
        mes = int(parte_fecha[4:6])
        return ano, mes

    # 4. Convertir fechas de inicio y fin a tuplas para comparar
    fecha_inicio = (ano_inicio, mes_inicio)
    fecha_fin = (ano_fin, mes_fin)

    # 5. Filtrar los archivos por el rango
    enlaces_filtrados = []
    for archivo in enlaces:
        try:
            ano_archivo, mes_archivo = extraer_fecha(archivo)
            fecha_archivo = (ano_archivo, mes_archivo)
            if fecha_inicio <= fecha_archivo <= fecha_fin:
                # Construir la URL completa
                url_completa = url_base + archivo
                enlaces_filtrados.append(url_completa)
        except (ValueError, IndexError):
            # Si algún archivo no tiene el formato esperado, lo ignoramos
            continue

    return enlaces_filtrados

    # Obtener enlaces desde enero de 1854 hasta diciembre de 1855
# enlaces = obtener_enlaces_ersst(1855, 12, 2025, 1)

# # Mostrar los primeros 5 resultados
# for url in enlaces[:5]:
#     print(url)
# print(f"Total de archivos encontrados: {len(enlaces)}")


def netcdf2cpt(path: str) -> str:

    # Leer archivo netCDF guardado en path
    try:
        ds = xr.open_dataset(path)
    except Exception as e:
        return f"Error al abrir el archivo: {e}"

    # Leer variable sst y recortar dimensiones Lat. y Lon.
    ds_subset = ds.sel(lat=slice(-30, 30), lon=slice(180, 340))
    sst_data = ds_subset['sst'].squeeze()
    
    latitudes = ds_subset['lat'].values
    longitudes = ds_subset['lon'].values
    
    # Extraer la fecha del nombre del archivo
    nombre_archivo = os.path.basename(path)
    
    # Patrón de 4 dígitos (año) seguidos de 2 dígitos (mes)
    match = re.search(r'(\d{4})(\d{2})', nombre_archivo)
    
    if match:
        year = match.group(1) # Los primeros 4 dígitos
        month = match.group(2) # Los siguientes 2 dígitos
        year_month = f"{year}-{month}"
    else:
        # Si por alguna razón el archivo no tiene números, se pone un valor por defecto
        year_month = "YYYY-MM" 

    #Convertir en formato .tsv (CPT)
    output_filename = path.replace('.nc', '_cpt.tsv')
    
    with open(output_filename, 'w') as f:
        # Encabezado estándar de CPT usando fecha del nombre
        f.write("xmlns:cpt=http://iri.columbia.edu/CPT/v10/\n")
        f.write("cpt:nfields=1\n")
        f.write(f"cpt:field=sst, cpt:T={year_month}, cpt:nrow={len(latitudes)}, cpt:ncol={len(longitudes)}, cpt:row=Y, cpt:col=X, cpt:units=Celsius, cpt:missing=-999.0\n")
        
        # Escribir la fila de longitudes
        lon_str = "\t" + "\t".join([f"{lon:.2f}" for lon in longitudes])
        f.write(lon_str + "\n")
        
        # Escribir cada fila de latitudes
        for i, lat in enumerate(latitudes):
            row_values = sst_data[i, :].fillna(-999.0).values
            row_str = f"{lat:.2f}\t" + "\t".join([f"{val:.3f}" for val in row_values])
            f.write(row_str + "\n")

    # Cerrar dataset
    ds.close()

    return output_filename

def download_file(url: str):
    #TODO: generar descarga del archivo en url y guardar en ./data
    #TODO: retornar ruta del archivo descargado ej: ./data/sst2026-01.nc
    return "path.nc"

def parallel_function(url: str):
    path=download_file(url)
    netcdf2cpt(path)

def integrate_file(paths: list[str]):
    #TODO: integrar todos los archivos .tsv generados en un solo archivo .tsv según lo solicitado por CPT
    #TODO: retornar ubicación del archivo consolidado
    return "file-CPT.tsv"