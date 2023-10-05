import sys

import requests
from bs4 import BeautifulSoup
import numpy as np
import pandas as pd
from datetime import datetime
import re
import psycopg2
from sqlalchemy import create_engine, text

user_dt = None
try:
    user_dt = sys.argv[1]
except Exception as e:
    print("No Date Specified")


# print(user_impt)

conn_dict = {'NAME': 'postgres',
             'USER': 'admin123',
             'PASSWORD': 'tensor123',
             'HOST': 'tensordb1.cn6gzof6sqbw.us-east-2.rds.amazonaws.com',
             'PORT': '5432', }

url = "https://rda.ucar.edu/datasets/ds084.1"
url_data_access = "https://rda.ucar.edu/datasets/ds084.1/dataaccess/"
root_url = "https://rda.ucar.edu"

def get_connection(host,port,user,passord,database):
    connection_string = f"postgresql://{user}:{passord}@{host}/{database}"
    db_connect = create_engine(connection_string)
    try:
        with db_connect.connect() as conn:
            result = conn.execute(text("SELECT 1"))
        print("\n\n---------------------Connection Successful")
        return db_connect
    except Exception as e:
        print("\n\n---------------------Connection Failed")
        print(e)

def get_connection_py(host,port,user,passord,database):
    connection_string = f"postgresql://{user}:{passord}@{host}/{database}"
    conn = psycopg2.connect(host=conn_dict['HOST'],
                            port=conn_dict['PORT'],
                            user=conn_dict['USER'],
                            password=conn_dict['PASSWORD'],
                            database=conn_dict['NAME'])
    try:
        curs = conn.cursor()
        res = curs.execute("Select 1")
        if res:
            curs.close()
        # print("\n\n---------------------Connection Successful")
        return conn
    except Exception as e:
        print(e)
        # print("\n\n---------------------Connection Failed")


insrt_conn = get_connection_py(host = conn_dict['HOST'],
                              port = conn_dict['PORT'],
                              user = conn_dict['USER'],
                              passord=conn_dict['PASSWORD'],
                              database=conn_dict['NAME'])

db_connection = get_connection(host = conn_dict['HOST'],
                              port = conn_dict['PORT'],
                              user = conn_dict['USER'],
                              passord=conn_dict['PASSWORD'],

                              database=conn_dict['NAME'])

def get_database_storage(conn):
    query = "SELECT * FROM files_data"
    df = pd.read_sql_query(query,conn)
    return df


files_df = get_database_storage(db_connection)
list_files = list(files_df['file_name']) ## List of all files in database
list_dates_db = list(files_df['date_for_hour'].dt.date)
files_df['for_date'] = files_df['date_for_hour'].dt.date
print(files_df['for_date'])

if len(files_df) > 1:

    files_df = files_df.drop('index',axis=1)
    files_df = files_df.sort_values('date_for_hour', ascending=False)
    list_files = list(files_df['file_name'])
    date_for_hour_list = list(files_df['date_for_hour'].unique())
    # files_df = files_df.loc[:, :].head(20)

try:
    r = requests.get(url_data_access)
except Exception as e:
    print("There was an issue on the website")
    print(e)

if r.status_code == 200:
    print("Request received")
else:
    print("There was issue with Data Access URL")

first_page = BeautifulSoup(r.content, 'html.parser')
data_access_matrix = first_page.find('div',class_='mtrx mx-1')
all_links = data_access_matrix.find_all('a')


print(f"Total Number of Links found on Data Access: {len(all_links)}")


# ge the link target got Web File Listings
get_a_for_table = None
for i in all_links:
    if str(i.get_text().strip()) == "Web FileListing":
        get_a_for_table = i
    else:
        pass


# Get the web file Listings
table_str = None
re_patt = r"([/].+/)+"
table_str = re.findall(pattern=re_patt,string=get_a_for_table['onclick'])[0]
if table_str != None:
    try:
        dp1 = requests.get(root_url + table_str)
    except Exception as e:
        print("There was an error in Website")
        print(e)

    if dp1.status_code == 200:
        print("Found the Link For Web File Listings")
        dp1 = BeautifulSoup(dp1.content,'html.parser')

dp1 = dp1.find('body')

on_click_a = dp1.find_all('a',onclick=True)

re_patt = r"([/].+filelist)+"  ## Pattern to get the Files List

file_list = None
for x in on_click_a:
    matched = re.findall(pattern=re_patt,string=x['onclick'])
    if len(matched)>0:
        file_list = matched[0]
if file_list != None:
    file_list = root_url + file_list
else:
    print("There is a change in the page for the Files List")

print(f"URL For Files List is f{file_list}")


# Get the Yearly Files data
if file_list != None:
    try:
        yearly_table = requests.get(file_list)
    except Exception as e:
        print(e)
        print("There was an issue with the website")

    if yearly_table.status_code == 200:
        yearly_table = BeautifulSoup(yearly_table.content,'html.parser')
    else:
        print("There is no table in the link",file_list)

yearly_table_body = yearly_table.find_all('tbody')
yearly_table_body = yearly_table_body[0]
dict_years = {'year':None,'link':None,'description':None} # create a dictionary for list of years available

years = []
links = []
description = []
yearly_a_tags = yearly_table_body.find_all('a')

for x in yearly_a_tags:
    lnk = root_url + x.get('href')
    links.append(lnk)
    years.append(int(x.get_text().strip()))
dict_years['year'] = years
dict_years['link'] = links

description_tags = yearly_table_body.find_all('td',class_='Description')
for x in description_tags:
    description.append(x.get_text().strip())
dict_years['description'] = description
yearly_dataframe = pd.DataFrame(dict_years)
yearly_dataframe = yearly_dataframe.sort_values('year',ascending=False)
print("This is the Yearly Dataframe \n\n")
print(yearly_dataframe)
print("\n")

latest_year = list(yearly_dataframe.loc[:,'year'])[0]
latest_link = list(yearly_dataframe.loc[:,'link'])[0]

print(f"Selecting the Latest Year f{int(latest_year)}\nWith the Latest Link f{latest_link}")

# Getting the latest Dates
print(f"Getting the dates from the latest year {latest_link}")
date_wise_links = requests.get(latest_link)
if date_wise_links.status_code == 200:
    date_wise_links = BeautifulSoup(date_wise_links.content,'html.parser')
    date_wise_links = date_wise_links.find('tbody')
else:
    print(f"There was an issue with the year link f{latest_link}")


dict_dates = {'dates':None,'links':None,'description':None}


dates = []
links = []
description = []

date_a_tags = date_wise_links.find_all('a')
for x in date_a_tags:
    lnk = root_url + x.get('href')
    links.append(lnk)
    dates.append(x.get_text().strip())

dict_dates['dates'] = dates
dict_dates['links'] = links

description_tags = date_wise_links.find_all('td',class_='Description')
for x in description_tags:
    description.append(x.get_text().strip())
dict_dates['description'] = description

dates_df = pd.DataFrame(dict_dates)
dates_df['dates']=pd.to_datetime(dates_df['dates'])
dates_df = dates_df.sort_values('dates',ascending=False)
print(f"This is the links and other details for the dates f{dates_df}")

latest_date = list(dates_df.loc[:,'dates'])[0]

latest_date_link = list(dates_df.loc[:,'links'])[0]

print(f"The latest Updated date is f{latest_date}\n\n")
print(f"The link for the latest date is \n{latest_date_link}")


def get_date_from_file(data):
    fl_date = datetime.strptime(data.split(".")[2],"%Y%m%d%H").strftime("%Y-%m-%d %H:%M:%S")
    return fl_date
def get_time_gap(data):
    time_gap = int(data.split(".")[3][1:])
    return time_gap

def insert_query(conn,files_data:dict):
    query = (f"""INSERT INTO files_data(dates, file_links, file_name,downloadable,size, time_delta, date_for_hour) VALUES (
                '{files_data['dates']}','{files_data['file_links']}','{files_data['file_name']}','{files_data['downloadable']}','{files_data['size']}'
                ,'{files_data['time_delta']}','{files_data['date_for_hour']}')""")
    curs = conn.cursor()
    try:
        curs.execute(query)
        conn.commit()
        curs.close()
        print(f"Inserted a new File {files_data['file_name']}")

    except Exception as e:
        print(e)
def get_date_list(dt):

    link = list(dates_df.loc[dates_df['dates']==dt,'links'])[0]
    print(link)
    try:
        list_link_resp = requests.get(link)
    except Exception as e:
        print(f"There is an issue at the link {link}")
        print(e)
        exit(345)
    if list_link_resp.status_code == 200:
        list_link_resp = BeautifulSoup(list_link_resp.content, 'html.parser')
        list_link_resp = list_link_resp.find('tbody')
    dates = []
    links = []
    file_name = []
    sizes = []
    files_final_dict = {'dates': None, 'file_links': None, 'file_name': None, 'downloadable': None, 'size': None}
    all_files_link = list_link_resp.find_all('a', attrs={'href': re.compile("^https://")})
    for x in all_files_link:
        links.append(x.get('href'))
        file_name.append(x.get_text().strip())
    size_tags = list_link_resp.find_all('td', class_='Size')
    for x in size_tags:
        sizes.append(x.get_text().strip())
    files_final_dict['dates'] = [None] * len(links)
    files_final_dict['file_links'] = links
    files_final_dict['file_name'] = file_name
    files_final_dict['size'] = sizes
    files_df = pd.DataFrame(files_final_dict)
    files_df['time_delta'] = files_df['file_name'].apply(get_time_gap)
    files_df['date_for_hour'] = files_df['file_name'].apply(get_date_from_file)
    files_df['dates'] = datetime.now().date()
    # return files_df
    latest_links_list = files_df.copy()
    # print(f"{files_df.loc[['file_name', 'file_links'], :]}")
    print(f"{files_df}")
    # print(latest_links_list)
    list_files_new = list(latest_links_list['file_name'])
    counter = 0
    for x in list_files_new:
        if x not in list_files:
            counter += 1
            dictionary = latest_links_list.loc[latest_links_list['file_name'] == x,
                         :].fillna('None').to_dict('records')
            if len(dictionary) > 0:
                dictionary = dictionary[0]
                insert_query(conn=insrt_conn, files_data=dictionary)

    if counter == 0:
        print("There are no more file changes")

latest_links_list = get_date_list(latest_date)

def get_list_from_db(dt):
    dt = dt.date()
    if dt not in list_dates_db:
        print("There are no files details in the database for the given date")

    else:
        req_files = files_df.loc[files_df['for_date'] == dt,:]

        print("Found the date")
        print(files_df)


if user_dt == None:
    pass
   # user_dt = input("Enter the date for searching the links in the form YYYY-MM-DD or say exit ")
   # if user_dt.strip().lower() == "exit":
    #    exit(654)
    # else:
      #  user_dt = datetime.strptime(user_dt,'%Y-%m-%d')
    # get_list_from_db(user_dt)


