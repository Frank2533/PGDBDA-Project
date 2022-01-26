# #Following script is used to gather company's market performance indices from website screener.in
# We have used selenium for this and also respected the websites bulk requests per account policy.
# Download location directory is also to be defined in the program.
# In this script a csv file with list of company Symbols is used to gather the data(here : scriptstock.csv)
# Also two separate log files are maintained ie. errorfetch.txt - data which failed to gather
#                                                filesgathered.txt - data that was successfully gathered. In this file the symbol , name of company, and time when it was downloaded is stored.


# Before running the program make sure the logs files are emptied.


import selenium
from selenium import webdriver
from selenium.webdriver import Keys
from selenium.webdriver.common import keys
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.common.keys import Keys
from datetime import datetime

import csv
import time

# datetime object containing current date and time

chromeOptions = webdriver.ChromeOptions()
prefs = {"download.default_directory": "/home/akash/Desktop/screenerdata2/data1/"}
chromeOptions.add_experimental_option("prefs", prefs)

chrome = webdriver.Chrome(options=chromeOptions)

screenerpage = chrome.get('https://www.screener.in/login/?')

userid = chrome.find_element_by_xpath('//*[@id="id_username"]')
userid.send_keys('frankenstien1947@gmail.com')
password = chrome.find_element_by_xpath('//*[@id="id_password"]')
password.send_keys('Akashsh12')
login = chrome.find_element_by_xpath('/html/body/main/div/div/div[2]/form/button')
login.click()
index = 0
file1 = open(r'/home/akash/Desktop/screenerdata2/errorfetch.txt', 'a')
file1.write(f'----------------------------------------------------------------\n')
file1.write(f'-----------FOLLOWING DATA WAS GATHERED ON: {datetime.now()}-----------------\n')
file1.write(f'----------------------------------------------------------------\n')

file2 = open(r'/home/akash/Desktop/screenerdata2/filesgathered.txt', 'a')
file2.write(f'----------------------------------------------------------------\n')
file2.write(f'-----------FOLLOWING DATA WAS GATHERED ON: {datetime.now()}-----------------\n')
file2.write(f'----------------------------------------------------------------\n')

with open('/home/akash/Desktop/screenerdata2/scriptstock.csv', 'rt') as f:
    data = csv.reader(f)
    for row in data:
        print(row)

        while True:
            try:
                gk = 0
                chrome.get(f'https://www.screener.in/company/{row[0]}')
                time.sleep(2)
                export = chrome.find_element_by_xpath('//*[@id="top"]/div[1]/form/button')

                export.click()
                # just to ensure if too many req page occurs.
                export = chrome.find_element_by_xpath('//*[@id="top"]/div[1]/form/button')
                index = index + 1

                file2.write(f'{row[0]},{row[1]},{datetime.now()}\n')

                print(f'gathered: {index}.{row[0]}')
                gk = gk + 1
            except selenium.common.exceptions.NoSuchElementException:
                try:
                    time.sleep(15)
                    if (gk == 0):
                        chrome.refresh()
                        export = chrome.find_element_by_xpath('//*[@id="top"]/div[1]/form/button')

                        export.click()
                        export = chrome.find_element_by_xpath('//*[@id="top"]/div[1]/form/button')
                        index = index + 1
                        file2.write(f'{row[0]},{row[1]},{datetime.now()}\n')
                        print(f'gathered: {index}.{row[0]}')
                        gk = gk + 1

                except selenium.common.exceptions.NoSuchElementException:
                    try:
                        if (gk == 0):
                            chrome.refresh()
                            chrome.get(f'https://www.screener.in/company/{row[0]}/consolidated/')
                            time.sleep(2)
                            export = chrome.find_element_by_xpath('//*[@id="top"]/div[1]/form/button')

                            export.click()
                            export = chrome.find_element_by_xpath('//*[@id="top"]/div[1]/form/button')
                            index = index + 1
                            file2.write(f'{row[0]},{row[1]},{datetime.now()}\n')
                            print(f'gathered: {index}.{row[0]}')
                            gk = gk + 1

                    except selenium.common.exceptions.NoSuchElementException:
                        file1.write(f'{row[0]},{row[1]}\n')
                        print(f'FAILED TO GATHER {index}{row[0]}')

            break;
file1.close()
f.close()









