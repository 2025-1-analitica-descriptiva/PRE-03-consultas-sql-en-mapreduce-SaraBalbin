"""Map/Reduce immplementation"""

# pylint: disable=broad-exception-raised

import fileinput
import glob
import os.path


def _load_input(input_directory):
    """
    Carga los archivos de entrada desde un directorio especificado.

    Lee todos los archivos dentro del directorio de entrada y guarda cada línea de los archivos
    junto con el nombre del archivo en una lista de tuplas.

    Parámetros:
        input_directory (str): Directorio que contiene los archivos de entrada.

    Retorna:
        list of tuples: Lista de tuplas (nombre_archivo, línea), donde cada tupla contiene el 
                        nombre del archivo y una línea de su contenido.
    """
    sequence = []
    files = glob.glob(f"{input_directory}/*")  # Obtiene todos los archivos en el directorio
    with fileinput.input(files=files) as f:  # Abre los archivos para su lectura
        for line in f:  # Itera sobre cada línea de cada archivo
            sequence.append((fileinput.filename(), line))  # Agrega el nombre del archivo y la línea a la secuencia
    return sequence


def _shuffle_and_sort(sequence):
    """
    Ordena la secuencia de entrada por la clave (primer elemento de cada tupla).

    Este paso simula el 'shuffle' y 'sort' típicos en un sistema MapReduce.

    Parámetros:
        sequence (list of tuples): Lista de tuplas (clave, valor) que debe ser ordenada.

    Retorna:
        list of tuples: Lista ordenada de tuplas, basada en el primer elemento de cada tupla.
    """
    return sorted(sequence, key=lambda x: x[0])  # Ordena la secuencia por la clave


def _create_output_directory(output_directory):
    """
    Crea el directorio de salida y limpia cualquier archivo existente.

    Si el directorio de salida ya existe, elimina todos los archivos y el directorio,
    luego lo vuelve a crear vacío.

    Parámetros:
        output_directory (str): Directorio donde se guardará la salida.
    """
    if os.path.exists(output_directory):
        for file in glob.glob(f"{output_directory}/*"):  # Elimina todos los archivos existentes en el directorio
            os.remove(file)
        os.rmdir(output_directory)  # Elimina el directorio vacío
    os.makedirs(output_directory)  # Crea el directorio de salida


def _save_output(output_directory, sequence):
    """
    Guarda la secuencia de salida en un archivo dentro del directorio de salida.

    La secuencia es guardada en un archivo llamado 'part-00000', con cada clave y valor
    separados por un tabulador.

    Parámetros:
        output_directory (str): Directorio donde se guardará el archivo de salida.
        sequence (list of tuples): Lista de tuplas (clave, valor) a guardar en el archivo.
    """
    with open(f"{output_directory}/part-00000", "w", encoding="utf-8") as f:
        for key, value in sequence:
            f.write(f"{key}\t{value}\n")  # Escribe cada par clave-valor en el archivo


def _create_marker(output_directory):
    """Crea un archivo marcador para indicar el éxito del proceso.

    Crea un archivo vacío llamado '_SUCCESS' en el directorio de salida para señalar que el job
    se completó con éxito.

    Parámetros:
        output_directory (str): Directorio donde se crea el archivo marcador.
    """
    with open(f"{output_directory}/_SUCCESS", "w", encoding="utf-8") as f:
        f.write("")  # El archivo se deja vacío para marcar el éxito del proceso


def run_mapreduce_job(mapper, reducer, input_directory, output_directory):
    """
    Orquesta el flujo completo de un job de MapReduce.

    Esta función ejecuta todas las fases del job MapReduce: carga los datos, aplica el mapper,
    realiza el 'shuffle and sort', aplica el reducer, guarda los resultados y crea el marcador
    de éxito.

    Parámetros:
        mapper (function): Función que aplica el mapeo sobre la secuencia de entrada.
        reducer (function): Función que aplica la reducción sobre la secuencia mapeada.
        input_directory (str): Directorio que contiene los archivos de entrada.
        output_directory (str): Directorio donde se guardarán los resultados.

    """
    sequence = _load_input(input_directory)  # Carga los datos de entrada
    sequence = mapper(sequence)  # Aplica el mapper a la secuencia
    sequence = _shuffle_and_sort(sequence)  # Realiza el 'shuffle' y 'sort' en la secuencia
    sequence = reducer(sequence)  # Aplica el reducer a la secuencia mapeada
    _create_output_directory(output_directory)  # Crea (o limpia) el directorio de salida
    _save_output(output_directory, sequence)  # Guarda los resultados en el directorio de salida
    _create_marker(output_directory)  # Crea el marcador de éxito para indicar que el job finalizó
