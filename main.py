import streamlit as st
from streamlit.components.v1 import html
import requests
from bs4 import BeautifulSoup
import numpy as np
import pandas as pd
from datetime import datetime
import re
import psycopg2
from streamlit_autorefresh import st_autorefresh


# sb = st.sidebar
st.set_page_config(layout="wide")
# update every 5 mins
st_autorefresh(interval=15 * 60 * 1000, key="dataframerefresh")

conn_dict = {'NAME': 'postgres',
             'USER': 'admin123',
             'PASSWORD': 'tensor123',
             'HOST': 'tensordb1.cn6gzof6sqbw.us-east-2.rds.amazonaws.com',
             'PORT': '5432', }
def get_connection(host,port,user,passord,database):
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
        print("\n\n---------------------Connection Successful")
        return conn
    except Exception as e:
        print(e)
        print("\n\n---------------------Connection Failed")
# print(conn.status)

conn = get_connection(host = conn_dict['HOST'],
                              port = conn_dict['PORT'],
                              user = conn_dict['USER'],
                              passord=conn_dict['PASSWORD'],
                              database=conn_dict['NAME'])

# print(conn)
if conn:
    st.success("Database Connection Successful")
else:
    st.warning("Some error occurred unsuccessful connection")

def get_database_storage(conn):
    query = "SELECT * FROM files_data"
    df = pd.read_sql_query(query,conn)
    return df

files_df = get_database_storage(conn)

  # List of files in database
list_files = list(files_df['file_name'])
# print(list_files)
if len(files_df) > 1:
    st.header("List of Latest Files Available")
    files_df = files_df.drop('index',axis=1)
    files_df = files_df.sort_values('date_for_hour', ascending=False)
    list_files = list(files_df['file_name'])
    # print(list_files)
    date_for_hour_list = list(files_df['date_for_hour'].unique())
    with st.sidebar:
        date_for_hour = st.selectbox(label="Select Date for Hour",options=date_for_hour_list)
    # files_df =  files_df.loc[files_df['date_for_hour']<=date_for_hour,:].head(20)
    files_df = files_df.loc[:, :].head(20)
    st.markdown(files_df.to_html(render_links=True,justify='center'),unsafe_allow_html=True)



## Get the list of Files present in database



st.title("GRF Files Updated")


def create_tables(table_body, table_for='year',url=None):
    if table_for == 'year':
        dict_files = {'year': None, 'link': None, 'description': None}
        years = []
        links = []
        description = []
        yearly_a_tags = table_body.find_all('a')
        for x in yearly_a_tags:
            lnk = root_url + x.get('href')
            links.append(lnk)
            years.append(int(x.get_text().strip()))
        dict_files['year'] = years
        dict_files['link'] = links
        description_tags = table_body.find_all('td', class_='Description')

        for x in description_tags:
            description.append(x.get_text().strip())
        dict_files['description'] = description
        yearly_dataframes = pd.DataFrame(dict_files)
        yearly_dataframes = yearly_dataframes.sort_values('year', ascending=False)
        return yearly_dataframes
    elif table_for == 'date':
        dict_files = {'dates': None, 'links': None, 'description': None}
        dates = []
        links = []
        description = []
        # date_wise_links = requests.get(url)
        date_a_tags = table_body.find_all('a')
        # print(table_body)
        for x in date_a_tags:
            lnk = root_url + x.get('href')
            links.append(lnk)
            dates.append(x.get_text().strip())
            dict_files['dates'] = dates
            dict_files['links'] = links
        description_tags = table_body.find_all('td', class_='Description')
        for x in description_tags:
            description.append(x.get_text().strip())
        dict_files['description'] = description
        dates_df = pd.DataFrame(dict_files)
        dates_df['dates'] = pd.to_datetime(dates_df['dates'])
        dates_df = dates_df.sort_values('dates', ascending=False)
        return dates_df



url = "https://rda.ucar.edu/datasets/ds084.1"
url_data_access = "https://rda.ucar.edu/datasets/ds084.1/dataaccess/"
root_url = "https://rda.ucar.edu"

st.markdown(f"<b>Primary URL is <a href='{url}'>{url}</a></b>", unsafe_allow_html=True)
r = requests.get(url_data_access)
if r.status_code == 200:
    st.success("Received data from the Primary URL")
    soup = BeautifulSoup(r.content, 'html.parser')
    data_access_matrix = soup.find('div', class_='mtrx mx-1')
    all_links = data_access_matrix.find_all('a')
    total_links_received = len(all_links)
    st.markdown(f"Total Links Received from Primary URL: <b>{total_links_received}</b>", unsafe_allow_html=True)

    get_a_for_table = None
    for i in all_links:
        if str(i.get_text().strip()) == "Web FileListing":
            get_a_for_table = i
            st.success("Found the Link for Web File Listings")
            table_str = None
            re_patt = r"([/].+/)+"
            table_str = re.findall(pattern=re_patt, string=get_a_for_table['onclick'])[0]
            if table_str != None:
                dp1 = requests.get(root_url + table_str)
                st.markdown(f"Link for Web Files Listings <a href='{root_url + table_str}'>{root_url + table_str}</a>",
                            unsafe_allow_html=True)
                if dp1.status_code == 200:
                    dp1 = BeautifulSoup(dp1.content, 'html.parser')  # Get the data from web files listings
                    # Got the page for data
                    dp1 = dp1.find('body')
                    re_patt = r"([/].+filelist)+"
                    file_list = None
                    on_click_a = dp1.find_all('a', onclick=True)
                    for x in on_click_a:
                        matched = re.findall(pattern=re_patt, string=x['onclick'])
                        if len(matched) > 0:
                            file_list = matched[0]  # Get the Files List
                            file_list = root_url + file_list
                            st.markdown(
                                f"The List of yearly files links can be found on <a href='{file_list}'>{file_list}</a>"
                                , unsafe_allow_html=True)
                            # create_tables()
                            if file_list != None:
                                yearly_table = requests.get(file_list)
                                if yearly_table.status_code == 200:
                                    yearly_table = BeautifulSoup(yearly_table.content, 'html.parser')
                                    ## table body
                                    table_body = yearly_table.find_all('tbody')
                                    len(table_body)
                                    table_body = table_body[0]
                                    df = create_tables(table_body)
                                    # df = df.reset_index()

                                    st.markdown("<h3>Yearly Links</h3>", unsafe_allow_html=True)
                                    st.markdown(df.to_html(render_links=True,justify='center'),unsafe_allow_html=True)

                                    st.markdown("<br><br>",unsafe_allow_html=True)
                                    years_list = list(df['year'])
                                    with st.sidebar:
                                        year_selected = st.selectbox(label="Select Year",options=years_list)

                                        # Get the Year url here
                                    st.markdown(f"<h5>Selected Year {year_selected}</h5>",unsafe_allow_html=True)
                                    year_url = list(df.loc[df['year']==year_selected,'link'])[0]
                                    # print(year_url)
                                    # Date Links are here
                                    date_wise_links = requests.get(year_url)
                                    date_wise_links = BeautifulSoup(date_wise_links.content,'html.parser')
                                    date_wise_links = date_wise_links.find('tbody')
                                    if date_wise_links != None:
                                        dict_dates = {'dates': None, 'links': None, 'description': None}
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
                                        description_tags = date_wise_links.find_all('td', class_='Description')
                                        for x in description_tags:
                                            description.append(x.get_text().strip())
                                        dict_dates['description'] = description
                                        dates_df = pd.DataFrame(dict_dates)
                                        dates_df['dates'] = pd.to_datetime(dates_df['dates'])
                                        dates_df = dates_df.sort_values('dates', ascending=False)
                                        with st.sidebar:
                                            date_list = list(dates_df['dates'])
                                            date_selected = st.selectbox(label='Select Date', options=date_list)
                                        dates_df2 = dates_df.loc[dates_df['dates']<=date_selected,:]
                                        st.markdown(dates_df2.head(10).to_html(render_links=True,justify='center'),unsafe_allow_html=True)
                                        ## This is for Sidebar Content


                                        latest_date = list(dates_df['dates'])[0]
                                        st.success(f"Latest Available Date on site is {latest_date}")
                                        latest_date_link = list(dates_df.loc[dates_df['dates']==latest_date,'links'])[0]

                                        st.markdown(f"Latest Date Files can be found on <a href='{latest_date}'>{latest_date_link}</a>",unsafe_allow_html=True)

                                        ## List of all the Files

                                        dates = []
                                        links = []
                                        file_name = []
                                        sizes = []
                                        files_final_dict = {'dates': None, 'file_links': None, 'file_name': None,
                                                            'downloadable': None, 'size': None}

                                        list_link_resp = requests.get(latest_date_link)
                                        # list_link_resp = requests.get('https://rda.ucar.edu/datasets/ds084.1/filelist/20230924')
                                        if list_link_resp.status_code == 200:
                                            list_link_resp = BeautifulSoup(list_link_resp.content, 'html.parser')
                                            list_link_resp = list_link_resp.find('tbody')


                                        all_files_link = list_link_resp.find_all('a', attrs={
                                            'href': re.compile("^https://")})
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

                                        df_latest_files = pd.DataFrame(files_final_dict)

                                        # file_namesx = files_final_dict['file_name']
                                        # for x in file_namesx:
                                        #     print(datetime.strptime(x.split('.')[2],'%Y%m%d%H').strftime("%Y-%m-%d %H:%M:%S"))
                                        def get_date_from_file(data):
                                            fl_date = datetime.strptime(data.split(".")[2],
                                                                        "%Y%m%d%H").strftime("%Y-%m-%d %H:%M:%S")

                                            return fl_date


                                        def get_time_gap(data):
                                            time_gap = int(data.split(".")[3][1:])
                                            return time_gap


                                        df_latest_files['time_delta'] = df_latest_files['file_name'].apply(get_time_gap)
                                        df_latest_files['date_for_hour'] = df_latest_files['file_name'].apply(get_date_from_file)
                                        # print(df_latest_files.head(5))
                                        df_latest_files['dates'] = datetime.now().date()

                                        list_files_new = list(df_latest_files['file_name'])
                                        # print(list_files_new)
                                        def insert_query(conn,files_data:dict,file_name):
                                            # data_to_send = files_data.loc[files_data['file_name']== file_name,:]
                                            # data_to_send = data_to_send.drop('index',axis=1)
                                            query = (f"""INSERT INTO files_data(dates, file_links, file_name,downloadable,size, time_delta, date_for_hour) VALUES (
                                                        '{files_data['dates']}','{files_data['file_links']}','{files_data['file_name']}','{files_data['downloadable']}','{files_data['size']}'
                                                        ,'{files_data['time_delta']}','{files_data['date_for_hour']}')""")
                                            # print(dictionary)
                                            # print(query)
                                            # st.text(query)
                                            curs = conn.cursor()
                                            try:
                                                curs.execute(query)
                                                conn.commit()
                                                curs.close()

                                                st.success(f"Inserted a new File Link to Database {file_name}")
                                            except Exception as e:
                                                st.warning(e)
                                                print(e)
                                        for x in list_files_new:
                                            if x not in list_files:
                                                # insert_query(conn=conn,file_name=x,files_data=df_latest_files)
                                                # dictionary = df_latest_files.loc[df_latest_files['time_delta'].isnull()==False ,:].fillna('None').to_dict('records')[0]
                                                # df_latest_files = df_latest_files.loc[df_latest_files['time_delta'].isnull() == False, :]
                                                # print(x in list_files)
                                                dictionary = df_latest_files.loc[df_latest_files['file_name'] == x,
                                                :].fillna('None').to_dict('records')
                                                # print(df_latest_files['time_to_hour'])
                                                if len(dictionary) > 0:
                                                    dictionary = dictionary[0]
                                                    insert_query(conn=conn, file_name=x, files_data=dictionary)
                                                # print(len(df_latest_files.loc[df_latest_files['file_name'] == x, :].to_dict('records')))
                                                # print(f"{x}\n\n")

                                                # insert_query(conn=conn, file_name=x, files_data=dictionary)


                                    else:
                                        st.warning("Internal Server Error Occurred Fom site Please Refresh")
                                else:
                                    st.warning("There is no table in the link", file_list)



            else:
                st.warning("Cannot Find Web Files Listings")

        else:
            pass
else:
    st.warning("Data From primary URL Not found")
