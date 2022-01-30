"""
Extraer la informacion del proveedor 1 desde la API de google sheets y exportarla en CSV
"""

import pygsheets
import pandas as pd

def main():
    # Autorización (El JSON tendria que estar en la carpeta)
    gc = pygsheets.authorize(service_file='./credentials_n2022.json')

    # Crear un DataFrame vacío
    df = pd.DataFrame()

    # Abir el Google Sheets (Estado_entregas es el nombre)
    sh = gc.open('Estado_entregas.xls')

    # Seleccionar la primer hoja
    df = sh[0].get_as_df()

    # Exportar en formato csv
    df.to_csv('./p1.csv', index = False, sep = ';')


if __name__ == "__main__":
    main()