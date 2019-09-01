import http.client
import logging
from logging import getLogger, StreamHandler, Formatter, FileHandler
from bs4 import BeautifulSoup
import time
import sys
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from datetime import datetime
from datetime import timedelta

# global var setting
APP_NAME = "get_tx_of_address"
DOMAIN = "www.blockchain.com"
FOLDER = "/ja/btc/address/{}?offset={}"

# logger setting
logLevel = logging.DEBUG
logger = getLogger(APP_NAME)
logger.setLevel(logLevel)
handler_format = Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
stream_handler = StreamHandler()
stream_handler.setLevel(logLevel)
stream_handler.setFormatter(handler_format)
logger.addHandler(stream_handler)
file_handler = FileHandler("{}{}.log".format("./log/" ,APP_NAME))
file_handler.setLevel(logLevel)
file_handler.setFormatter(handler_format)
logger.addHandler(file_handler)

def get_page_of_tx(address="", page_num="0"):
    if address == "":
        return None
    res = ""
    while True:
        con = http.client.HTTPSConnection(DOMAIN)
        con.request("GET", FOLDER.format(address, page_num))
        res = con.getresponse()
        if res.status == 200:
            break
        else:
            time.sleep(2)
    html_text = res.read()
    html_status = res.status
    con.close()
    return html_status, html_text

def get_number_of_txs(address):
    html_status, html_text = get_page_of_tx(address)
    if html_status == None:
        logger.warning("please enter address into argument")
        return 0
    soup = BeautifulSoup(html_text, 'html.parser')
    num_of_txs = soup.find(id="n_transactions")
    return int(num_of_txs.text)

def get_element_of_tx(address,num_of_txs):
    time_list = []
    num_of_txs += 49
    for num in range(0, num_of_txs, 50):
        html_status, html_text = get_page_of_tx(address, num)
        if html_status == None:
            logger.warning("please enter address into argument")
            return None
        soup = BeautifulSoup(html_text, 'html.parser')
        txs = soup.find_all(class_="txdiv")
        for tx in txs:
            time_soups = tx.find_all(class_="pull-right")
            for time_soup in time_soups:
                time = time_soup.text
                if "BTC" not in time.split(" "):
                    tmp_time_dict = {}
                    tmp_time_dict["time"] = time
                    time_list.append(tmp_time_dict)
    return time_list

def get_address_data(address):
    logger.info("======================")
    logger.info(address)
    num_of_txs = get_number_of_txs(address)
    time_list = get_element_of_tx(address, num_of_txs)
    df_time = add_col_hh_to_df_time(pd.DataFrame(time_list))
    df_time['YYYY-MM-DD'] = pd.to_datetime(df_time['YYYY-MM-DD'])
    df_time.sort_values(by='YYYY-MM-DD', inplace=True)
    df_time.reset_index(inplace=True, drop=True)
    return df_time



def add_col_hh_to_df_time(df_time):
    df_time = pd.concat([df_time, df_time["time"].str.split(" ", n=2, expand=True)], axis=1)
    df_time.rename(columns={0:"YYYY-MM-DD",1:"hh:mm:ss"},inplace=True)
    df_time = pd.concat([df_time, df_time["YYYY-MM-DD"].str.split("-", n=2, expand=True)], axis=1)
    df_time.rename(columns={0:"YYYY",1:"MM",2:"DD"},inplace=True)
    df_time = pd.concat([df_time, df_time["hh:mm:ss"].str.split(":", n=2, expand=True)], axis=1)
    df_time.rename(columns={0:"hh",1:"mm",2:"dd"},inplace=True)
    df_time["hh"] = df_time.hh.astype('int32')
    return df_time


def insert_data():
    pass

def make_graph_of_address_by_hour(df_time, address, time_zone):
    tx_count_list = []
    for i in range(24):
        time = (i - time_zone + 24) % 24
        tx_count_dict = {}
        tx_count_dict["count"] = df_time[df_time.hh == time]["hh"].count()
        tx_count_list.append(tx_count_dict)
    count_df_time = pd.DataFrame(tx_count_list)
    plt.title("{}".format(address))
    plt.xlabel("hour")
    plt.ylabel("count")
    plt.plot(count_df_time["count"])
    plt.savefig("{}_by_hour.png".format(address))
    plt.clf()

def make_df_count_from_df_time(df_time):
    first_day = df_time.iloc[0]['YYYY-MM-DD'] - timedelta(days=1)
    last_day = df_time.iloc[-1]['YYYY-MM-DD'] + timedelta(days=2)
    df_graph = df_time['YYYY-MM-DD'].value_counts(sort=True, ascending=True)
    date_list = []
    count_list = []
    graph_date_list = df_graph.index.astype("str").tolist()
    num_of_days = 0
    while first_day != last_day:
        date_list += [first_day.strftime("%Y-%m-%d")]
        if first_day.strftime("%Y-%m-%d") in graph_date_list:
            count_list.append(df_graph[first_day.strftime("%Y-%m-%d")])
        else:
            count_list.append(0)
        first_day += timedelta(days=1)
        num_of_days += 1
    return pd.DataFrame(data={'date': date_list, 'count': count_list}, columns=['date', 'count']), df_time.iloc[0]['YYYY-MM-DD'].strftime("%Y-%m-%d %H:%M:%S"), num_of_days

def make_graph_of_address_by_day(df_time, address, time_zone):
    df_count, first_day, num_of_days = make_df_count_from_df_time(df_time)
    df_count['date'] = pd.to_datetime(df_count['date'])
    x1 = pd.date_range(first_day, periods=num_of_days,freq='d')
    fig = plt.figure(figsize=(20,8))
    ax = fig.add_subplot(1,1,1)
    ax.plot(x1, df_count["count"])
    ax.set_xlabel('date')
    ax.set_ylabel('count')
    ax.set_title('{}'.format(address))
    plt.savefig("{}_by_days.png".format(address))
    plt.clf()


def main():
    args = sys.argv
    time_zone = -4
    address = args[1]
    df_time = get_address_data(address)
    make_graph_of_address_by_day(df_time, address, time_zone)
    make_graph_of_address_by_hour(df_time, address, time_zone)

if __name__ == "__main__":
    main()
