import pandas as pd
import numpy as np
import xlrd
from datetime import datetime

""" 
 Files path:
    -   p1.csv  --> It comes from etl_p1.py
    -   p2.csv  --> It is received by mail 1 time a day
""" 

k_workbook = './p1.csv'
b_workbook = './p2.xlsx'

def ETL_logistica_1(k_workbook:str):
    ## ETL Logistics 1
    # Import
    book_k = pd.read_csv(file1, sheet_name="ENTREGAS")


    # Clear rows
    book_k = book_k.dropna(subset=["PREPARACION"])
    book_k = book_k.loc[book_k['PREPARACION'].str.contains('ENTREG')]

    book_k['Fecha_entrega'] = book_k["PREPARACION"].apply(lambda x: '{}/{}/2021'.format(x[-5:-3],x[-2:]))

    book_k = pd.concat([book_k['RTO'],book_k['Nombre'],book_k['DNI'],book_k['Fecha_entrega'],book_k['Comentarios'],book_k['col_provincia'],book_k['col_localidad']], axis=1)
    book_k['Proveedor'] = 'P1'

    book_k = book_k.drop(book_k[(book_k['Fecha_entrega']=='DO/BK/2021')].index)

    # Filter
    book_k['Fecha_entrega'] = pd.to_datetime(book_k['Fecha_entrega'], format='%d/%m/%Y')
    out_k = book_k[book_k['Fecha_entrega'] >= fechas[0]]

    return out_k

def ETL_logistica_2(b_workbook:str):
    ## ETL Logistics 2
    # Import
    file2 = pd.ExcelFile(b_workbook)
    book_b = pd.read_excel(file2, sheet_name="Listado")

    # Clear rows
    book_b = book_b.dropna(subset=["ENTREGADO"])

    book_b['Comentarios'] = ''
    book_b['Proveedor'] = 'P2'

    book_b = pd.concat([book_b['Nro de Remito'],book_b['Name'],book_b['Employee ID'],book_b['ENTREGADO'],book_b['Comentarios'],book_b['col_provincia'],book_b['col_localidad'],book_b['Proveedor']], axis=1)

    book_b['Nro de Remito'] = book_b["Nro de Remito"].apply(lambda x: x[-5:])

    # Filter
    out_b = book_b[book_b['ENTREGADO'] >= fechas[0]]

    return out_b

def main(k_workbook, b_workbook):
    """
    
    """

    out_k = ETL_logistica_1(k_workbook)
    out_b = ETL_logistica_2(b_workbook)

    # Column normalization
    out_b.set_axis(out_k.columns, 
                        axis='columns', inplace=True)

    out_df = out_k.append(out_b)
    del out_k
    del out_b

    # Export xlsx
    out_df.to_excel('./out/out_entregas.xlsx',index = False)


if __name__ == "__main__":
    main(k_workbook, b_workbook)
