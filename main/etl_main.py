import pandas as pd
import numpy as np
import xlrd
from datetime import datetime

""" 
Path de archivos:
    -   p1.csv  --> Viene de etl_p1.py
    -   p2.csv  --> Viene por mail 1 vez al dia
""" 

k_workbook = './p1.csv'
b_workbook = './p2.xlsx'

def ETL_logistica_1(k_workbook):
    ##### ETL Logistica 1
    ### Importar
    book_k = pd.read_csv(file1, sheet_name="ENTREGAS")


    ### Limpiar filas
    # Dejo las entregas
    book_k = book_k.dropna(subset=["PREPARACION"])
    book_k = book_k.loc[book_k['PREPARACION'].str.contains('ENTREG')]

    # Modificar y formatear a fecha
    book_k['Fecha_entrega'] = book_k["PREPARACION"].apply(lambda x: '{}/{}/2021'.format(x[-5:-3],x[-2:]))

    # Dejar las columnas que sirven
    book_k = pd.concat([book_k['RTO'],book_k['Nombre'],book_k['DNI'],book_k['Fecha_entrega'],book_k['Comentarios'],book_k['col_provincia'],book_k['col_localidad']], axis=1)
    book_k['Proveedor'] = 'P1'

    #Elimino casos que no me sirven --> 'DO/BK/2021'
    book_k = book_k.drop(book_k[(book_k['Fecha_entrega']=='DO/BK/2021')].index)

    # Filtro fechas a última semana
    book_k['Fecha_entrega'] = pd.to_datetime(book_k['Fecha_entrega'], format='%d/%m/%Y')
    out_k = book_k[book_k['Fecha_entrega'] >= fechas[0]]

    return out_k

def ETL_logistica_2(b_workbook):
    ##### Logistica 2
    ### Importar
    file2 = pd.ExcelFile(b_workbook)
    book_b = pd.read_excel(file2, sheet_name="Listado")

    ### Limpiar filas
    # Dejo las entregas
    book_b = book_b.dropna(subset=["ENTREGADO"])

    #Agrego columnas para comentarios y proveedor
    book_b['Comentarios'] = ''
    book_b['Proveedor'] = 'P2'

    # Dejar las columnas que sirven
    book_b = pd.concat([book_b['Nro de Remito'],book_b['Name'],book_b['Employee ID'],book_b['ENTREGADO'],book_b['Comentarios'],book_b['col_provincia'],book_b['col_localidad'],book_b['Proveedor']], axis=1)

    # Corrijo columna Remitos
    book_b['Nro de Remito'] = book_b["Nro de Remito"].apply(lambda x: x[-5:])

    # Filtro fechas a última semana
    out_b = book_b[book_b['ENTREGADO'] >= fechas[0]]

    return out_b

def main(k_workbook, b_workbook):
    """
    
    """

    out_k = ETL_logistica_1(k_workbook)
    out_b = ETL_logistica_2(b_workbook)

    # Normalizo nombre de columnas
    out_b.set_axis(out_k.columns, 
                        axis='columns', inplace=True)

    # Genero un nuevo df combinando ambas dfs
    out_df = out_k.append(out_b)

    # Elimino dfs anteriores
    del out_k
    del out_b

    # Exporto la informaacion final a un excel, solicitado en este formato,
    # listo para enviar a las personas asignadas y ser leido desde tableau
    out_df.to_excel('./out/out_entregas.xlsx',index = False)


if __name__ == "__main__":
    main(k_workbook, b_workbook)
