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
add_all_data = None
try:
    user_dt = sys.argv[1]
except Exception as e:
    print("No Date Specified")

try:
    add_all_data = sys.argv[2]
except Exception as e:
    # print("Will be adding single file at a time")
        print(e)



url = "https://rda.ucar.edu/datasets/ds084.1"
url_data_access = "https://rda.ucar.edu/datasets/ds084.1/dataaccess/"
root_url = "https://rda.ucar.edu"

year_2023_url = "https://rda.ucar.edu/datasets/ds084.1/filelist/2023"

conn_dict = {'NAME': 'postgres',
             'USER': 'admin123',
             'PASSWORD': 'tensor123',
             'HOST': 'tensordb1.cn6gzof6sqbw.us-east-2.rds.amazonaws.com',
             'PORT': '5432', }

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


# Get the Connection for inserting in datbase
insrt_conn = get_connection_py(host = conn_dict['HOST'],
                              port = conn_dict['PORT'],
                              user = conn_dict['USER'],
                              passord=conn_dict['PASSWORD'],
                              database=conn_dict['NAME'])

# Get the Database readers for pandas
db_connection = get_connection(host = conn_dict['HOST'],
                              port = conn_dict['PORT'],
                              user = conn_dict['USER'],
                              passord=conn_dict['PASSWORD'],

                              database=conn_dict['NAME'])

# Get the files data stored in database
def get_database_storage(conn):
    query = "SELECT * FROM files_gfs"
    df = pd.read_sql_query(query,conn)
    return df

files_df = get_database_storage(db_connection)
# print(files_df.info())

# Get the Files data from the database
if len(files_df) > 1:
    list_files = list(files_df['file_name']) ## List of all files in database
    list_dates_db = list(files_df['date_for_hour'].dt.date)
    files_df['for_date'] = files_df['date_for_hour'].dt.date
    files_df = files_df.drop('index',axis=1)
    files_df = files_df.sort_values('date_for_hour', ascending=False)
    list_files = list(files_df['file_name'])
    date_for_hour_list = list(files_df['date_for_hour'].unique())

try:
    r = requests.get(year_2023_url)
    print(r.status_code)
    
except Exception as e:
    print("Cannot Find Data fron site")
    print(e)


if r.status_code == 200:
    print("Request received")
    date_wise_links = BeautifulSoup(r.content,'html.parser') 
    date_wise_links = date_wise_links.find('tbody')
    # print(date_wise_links)
else:
    print("There was issue with Data Access URL")
    exit(500)

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
print(f"This is the latest links and other details for the dates \n{dates_df.head(2).loc[:,['dates','links']]}")


# dates_dict = dates_df.head(2).to_dict()
# print(dates_dict)
print("Choosing the Latest date Here")
latest_date = list(dates_df.loc[:,'dates'])[0]

latest_date_link = list(dates_df.loc[:,'links'])[0]

print(f"The latest Updated date is {latest_date}\n\n")
print(f"The link for the latest date is \n{latest_date_link}")

def get_date_from_file(data):
    fl_date = datetime.strptime(data.split(".")[2],"%Y%m%d%H").strftime("%Y-%m-%d %H:%M:%S")
    return fl_date
def get_time_gap(data):
    time_gap = int(data.split(".")[3][1:])
    return time_gap


## Insert Query
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


# Get files for a date from website
def get_date_list(dt):
    link = list(dates_df.loc[dates_df['dates']==dt,'links'])[0] # Accepts %Y-%m-%d %H:%M:%S
    try:
        list_link_resp = requests.get(link)
    except Exception as e:
        print(f"There is an issue at the link {link}")
        print(e)
        # get_date_list(dt)
        exit(354)
    if list_link_resp.status_code == 200:
        list_link_resp = BeautifulSoup(list_link_resp.content, 'html.parser')
        list_link_resp = list_link_resp.find('tbody')
    # dates = []
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
    files_df['dates'] = dt
    files_df['log_ts'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    files_df.sort_values(['date_for_hour','time_delta'],ascending=[False,False],inplace=True)
    print(f"{files_df}")
    return(files_df)

class GetListofFiles:
    def __init__(self,dt) -> None:
        self.dt = dt
        self.in_db = False
        self.dt2 = dt.date()
        self.send_df = None
    def get_list(self):
        if self.dt2 not in list_dates_db:
            print("There are no files details in the database for the given date")
            print("Getting list of files from website")
            self.send_df = get_date_list(dt = self.dt)
            return self.send_df
        else:
            self.in_db = True
            print("Found Date")
            self.send_df = files_df.loc[files_df['dates'] == self.dt,:]
            return self.send_df


if user_dt !=None:
    user_dt = datetime.strptime(user_dt,'%Y-%m-%d')
    # get_list = GetListofFiles(user_dt)
    get_list = get_date_list(user_dt)
else:
    # get_list = GetListofFiles(user_dt)
    get_list = get_date_list(latest_date)
print(get_list)

list_new = list(get_list['file_name'])
# list_files
in_db_list = []

for x in list_new:
    if x not in list_files:
        in_db_list.append(True)
    else:
        in_db_list.append(False)
print(f"The Total Number of new Files found are")
get_list = get_list.loc[in_db_list,:]
try:
    get_list.to_sql(name='files_gfs',con=db_connection,if_exists='append')
except Exception as e:
    print("Error at the SQL insert Query")
    print(e)
if len(get_list) == 0:
    print("There are no new files to add")
else:
    print(f"Number of new files added are {len(get_list)}")
    print(get_list.loc[:,['file_name','log_ts']])
