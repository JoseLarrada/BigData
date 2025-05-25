import os
import csv
import sys

# Aumentar el límite de tamaño de campo
csv.field_size_limit(sys.maxsize)

def dividir_csv(input_path, output_dir, filas_por_archivo=100000):
    os.makedirs(output_dir, exist_ok=True)
    archivo_num = 0
    filas_en_archivo = 0
    archivo_salida = None
    escritor_csv = None

    with open(input_path, mode='r', encoding='utf-8') as f_in:
        lector = csv.reader(f_in)
        encabezado = next(lector)

        for fila in lector:
            if filas_en_archivo == 0:
                if archivo_salida:
                    archivo_salida.close()
                nombre_archivo = os.path.join(output_dir, f"parte_{archivo_num:04d}.csv")
                archivo_salida = open(nombre_archivo, mode='w', encoding='utf-8', newline='')
                escritor_csv = csv.writer(archivo_salida)
                escritor_csv.writerow(encabezado)
                archivo_num += 1

            escritor_csv.writerow(fila)
            filas_en_archivo += 1

            if filas_en_archivo >= filas_por_archivo:
                filas_en_archivo = 0

        if archivo_salida:
            archivo_salida.close()

    print(f"✔ División completa: {archivo_num} archivos creados en {output_dir}")

# Ejemplo de uso
input_csv = "data/Visitas_lote_02.csv"
output_folder = "data/Visitas_lote_02_dividido"
dividir_csv(input_csv, output_folder, filas_por_archivo=100000)
