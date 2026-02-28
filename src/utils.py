import requests
from bs4 import BeautifulSoup
from typing import List, Tuple

def obtener_enlaces_ersst(
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