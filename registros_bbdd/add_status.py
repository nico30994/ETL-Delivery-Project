import pandas as pd
from datetime import datetime

from sqlalchemy import create_engine
import psycopg2
import psycopg2.extras as extras


def main():
    ###########
    ### PATCH = Carpeta DATA con EXCEL
    patchEXC = './out/out_entregas.xlsx'
    columns=['col_DNI','col_ruteado', 'col_estado', 'col_fecha_entrega', 'col_nro_rto', 'col_proveedor']
    table_name = 'Status_roster'

    ### Abrir archivo maestro
    try:
        df = pd.read_excel(patchEXC, names=columns)
    except:
        print('Error, no existe el archivo')

    df = df_format(df)

    connect_bbdd(df,table_name)


def connect_bbdd(df,table_name):
    """
    Conectarse a la BBDD, BBDD_USER y BBDD_PASS borrados
    """
    conn = psycopg2.connect(
                host = "localhost",
                database="Accenture_Project",
                user = "BBDD_USER",
                password = "BBDD_PASS")


    cols_names = df.columns
    table = 'public."{}"'.format(table_name)

    execute_batch(conn, df, table, cols_names, page_size=100)


def execute_batch(conn, df, table, cols_names, page_size=100):
    """
    Using psycopg2.extras.execute_batch() to insert the dataframe
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



def df_format(df):
    # Cambiar formato para ser leidos en posgres
    df['col_nro_rto'] = pd.to_numeric(df['col_nro_rto'], errors='coerce')
    df['col_fecha_entrega'] = pd.to_datetime(df['col_fecha_entrega'], errors='coerce')
    df = df.replace({np.nan: None})
    return df




if __name__ == "__main__":
    main()

