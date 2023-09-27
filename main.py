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
conn_dict = {'ENGINE': 'django.db.backends.postgresql',
             'NAME': 'postgres',
             'USER': 'admin123',
             'PASSWORD': 'tensor123',
             'HOST': 'tensordb1.cn6gzof6sqbw.us-east-2.rds.amazonaws.com',
             'PORT': '5432', }
conn = psycopg2.connect(host=conn_dict['HOST'],
                        port=conn_dict['PORT'],
                        user=conn_dict['USER'],
                        password=conn_dict['PASSWORD'],
                        database=conn_dict['NAME'])
# print(conn.status)


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
        print(table_body)
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


if conn.status == 1:
    st.success("Database Connection Successful")
else:
    st.warning("Some error occurred unsuccessful connection")

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
                                        st.markdown(dates_df.head(10).to_html(render_links=True,justify='center'),unsafe_allow_html=True)

                                        date_list = list(dates_df['dates'])
                                        ## This is for Sidebar Content

                                        with st.sidebar:
                                            st.selectbox(label='Select Date',options=date_list)
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
