import pandas as pd
from datetime import datetime

from sqlalchemy import create_engine
import psycopg2
import psycopg2.extras as extras


def main():
    ###########
    ### PATCH = Carpeta DATA con EXCEL
    patchEXC = 'C:\\Users\\nico_\\Documents\\ETL Entregas\\Excels\\Originales\\add_Roster.xlsx'
    columns=['DNI','Employee_ID','col_name','col_provincia','col_localidad','nombre_calle','altura_direccion','codigo_postal','col_format','col_level','col_mail']

    ### Abrir archivo maestro
    try:
        df = pd.read_excel(patchEXC, names=columns)
    except:
        print('Error, no existe el archivo')
    
    connect_bbdd(df)



def connect_bbdd(df):
    """
    Conectarse a la BBDD, BBDD_USER y BBDD_PASS borrados
    """
    conn = psycopg2.connect(
                host = "localhost",
                database="A_Project",
                user = "BBDD_USER",
                password = "BBDD_PASS")


    cols_names = df.columns
    table = 'public."Roster"'

    execute_batch(conn, df, table, cols_names, page_size=100)


def execute_batch(conn, df, table, cols_names, page_size=100):
    """
    Usando psycopg2.extras.execute_batch() se inserta el DataFrame
    """
    # Create a list of tupples from the dataframe values
    tuples = [tuple(x) for x in df.to_numpy()]
    # Convertir nombre de columnas
    cols = '"' + '","'.join(list(cols_names)) + '"'
    # %s-separated values
    values = "VALUES({})".format(",".join(["%s" for _ in list(df)])) 
    #Update SET values
    set_values = 'SET "{}"'.format(",".join(str(_) + ' = EXCLUDED."' + str(_) + '"' for _ in list(df)[:-1])) 
    # SQL quert to execute
    query  = """
    INSERT INTO {}({}) {} 
    ON CONFLICT ("{}") DO NOTHING
    """.format(table, cols,values,cols_names[0])
    print(query)
    cursor = conn.cursor()
    try:
        extras.execute_batch(cursor, query, tuples, page_size)
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: {}".format(error))
        conn.rollback()
        cursor.close()
        return 1
    print("execute_batch() done")
    cursor.close()


if __name__ == "__main__":
    main()