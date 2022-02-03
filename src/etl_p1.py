import pygsheets
import pandas as pd

"""
Extract the information of P1 from the google sheets API and export it in CSV -> p1.csv
"""

def main():
    gc = pygsheets.authorize(service_file='./credentials_n2022.json')
    df = pd.DataFrame()
    sh = gc.open('Estado_entregas.xls')
    df = sh[0].get_as_df()
    df.to_csv('./p1.csv', index = False, sep = ';')


if __name__ == "__main__":
    main()