import csv
import os
import pandas as pd
import xlrd
from xlrd import XLRDError
from fuzzywuzzy import process
import openpyxl
import Levenshtein

def excel_rename(path):
    for filename in os.listdir(path):
        try:
            filepath = os.path.join(path, filename)
            excel_file = xlrd.open_workbook(filepath)
            worksheet = excel_file.sheet_by_index(5)
            new_filename=worksheet.cell_value(0,1)
            x=new_filename.title()
            m=x.split(' ')
            print(m)
            m=list(map(lambda y: y.replace('Ltd','Limited'), m))
            j=(' ').join(m)
            print(j)
            os.rename(filepath, os.path.join(path, j+'.xlsx'))
        except XLRDError:
            print(j+" renaming failed")
            continue



def excel_rename_symbol(path, newpath, scriptpath):
    data = pd.read_csv(f'{scriptpath}')
    data.drop_duplicates(subset='NAME OF COMPANY', keep='first', inplace=True)
    print(data)
    data.to_csv(f'{scriptpath}', index=False)
    i=0
    IDs={}
    with open(scriptpath) as f:
        data = csv.reader(f)
        for row in data:

            k=row[1].lower()
            IDs[k] = row[0]


    for filename in os.listdir(path):
        print(filename)
        try:
            filepath = os.path.join(path, filename)
            x = os.path.splitext(filename.lower())
            print(x)
            os.rename(filepath, os.path.join(newpath, f'{IDs[x[0]]}.xlsx'))
        except KeyError:
            i = i + 1
            # print(f'{i}.{filename} Reaname failed')
            filepath = os.path.join(path, filename)
            x = os.path.splitext(filename.lower())
            print(x[0]+str(i)+": FAILED BUT RETRYING USING BEST MATCH ")
            keys_list=list(IDs.keys())
            query = x[0]
            Best_match, perc=process.extractOne(query, keys_list)
            os.rename(filepath, os.path.join(newpath, f'{IDs[Best_match]}.xlsx'))
            print(str(i)+": Changed "+x[0]+" Sucessfully using best match with "+str(perc)+"% match")



def xlsxtocsvextraction(path,newPath,pathToStockData):
    # with open('/home/akash/Desktop/DbdaProject (copy)/PGDBDA-Project/Extract/step10/Symbolnamemap.csv', 'rt') as f:
    #     data = csv.reader(f)
    #     for row in data:
    #         print(row[0])
    for filename in os.listdir(path):
        print(filename)
        x = os.path.splitext(filename)
        print(x)

        try:                                                                                #This block ensures the file is present in stock data and only the acquires the
                                                                                            #company data files.
            df_comp = pd.read_csv(
                f"{pathToStockData}/{x[0]}.csv")
            df_comp['Company_Name'] = x[0]
            df_comp.to_csv(
                f'{newPath}/{x[0]}.csv')  # stockdata file
        except FileNotFoundError:
            continue

        df_Pr_Lo = pd.read_excel(
            f'{path}/{x[0]}.xlsx',
            sheet_name='Data Sheet', header=15, index_col=0, nrows=14)
        df_Pr_Lo['Company_Name'] = x[0]
        df_Pr_Lo.to_csv(
            f'{newPath}/{x[0]}_Po_Lo.csv')  # ProfitLossTable



        df_bal = pd.read_excel(
            f'{path}/{x[0]}.xlsx',
            sheet_name='Data Sheet', header=55,
            index_col=0, nrows=16)
        df_bal['Company_Name'] = x[0]
        df_bal.to_csv(
            f'{newPath}/{x[0]}_bal.csv')  # Balancesheet table


        df_quar = pd.read_excel(
            f'{path}/{x[0]}.xlsx',
            sheet_name='Data Sheet',
            header=40,
            index_col=0, nrows=9)
        df_quar['Company_Name'] = x[0]
        df_quar.to_csv(
            f'{newPath}/{x[0]}_quar.csv')  # quartile Table

        df_cash = pd.read_excel(
            f'{path}/{x[0]}.xlsx',
            sheet_name='Data Sheet', header=80,
            index_col=0, nrows=4)
        df_cash['Company_Name'] = x[0]
        df_cash.to_csv(
            f'{newPath}/{x[0]}_cash.csv')  # cash flow Table







if __name__ == '__main__':
    excel_rename('/home/akash/Desktop/DbdaProject (4th copy)/PGDBDA-Project/Extract/step2-9/screenerdata2/data1')   #Rename the files according to company name
    excel_rename_symbol('/home/akash/Desktop/DbdaProject (4th copy)/PGDBDA-Project/Extract/step2-9/screenerdata2/data1','/home/akash/Desktop/DbdaProject (4th copy)/PGDBDA-Project/Extract/step2-9/screenerdata2/data1renamed', '/home/akash/Desktop/DbdaProject (4th copy)/PGDBDA-Project/Extract/step2-9/screenerdata2/EQUITY_L (NSE) (trimmed).csv')
    xlsxtocsvextraction('/home/akash/Desktop/DbdaProject (4th copy)/PGDBDA-Project/Extract/step2-9/screenerdata2/data1renamed','/home/akash/Desktop/DbdaProject (4th copy)/PGDBDA-Project/Extract/step2-9/screenerdata2/xlsxtocsvconverted','/home/akash/Desktop/DbdaProject (4th copy)/PGDBDA-Project/Extract/step1/DataGatherStock/data')
    print("-------------------- ALL FILES EXTRACTION DONE -------------------------")
        
