import http.client
import logging
from logging import getLogger, StreamHandler, Formatter, FileHandler
from bs4 import BeautifulSoup
import time
import pandas as pd
import matplotlib.pyplot as plt

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
    return pd.DataFrame(time_list)


def insert_data():
    pass

def make_graph_of_address(df, address, time_zone):
    df = pd.concat([df, df["time"].str.split(" ", n=2, expand=True)], axis=1)
    df.rename(columns={0:"YYYY-MM-DD",1:"hh:mm:ss"},inplace=True)
    df = pd.concat([df, df["YYYY-MM-DD"].str.split("-", n=2, expand=True)], axis=1)
    df.rename(columns={0:"YYYY",1:"MM",2:"DD"},inplace=True)
    df = pd.concat([df, df["hh:mm:ss"].str.split(":", n=2, expand=True)], axis=1)
    df.rename(columns={0:"hh",1:"mm",2:"dd"},inplace=True)
    df["hh"] = df.hh.astype('int32')
    tx_count_list = []
    for i in range(24):
        time = (i - time_zone + 24) % 24
        tx_count_dict = {}
        tx_count_dict["count"] = df[df.hh == time]["hh"].count()
        tx_count_list.append(tx_count_dict)
    count_df = pd.DataFrame(tx_count_list)
    plt.plot(count_df["count"])
    plt.savefig("{}.png".format(address))
    return df

def main():
    address = "35pJQef1CGscLec9jyddMu2DLU5Swq12wK"
    address = "19vNKwvSHTvGYUtftUTe7haaTYLdk742Bm"
    df = get_address_data(address)
    time_zone = -4
    df = make_graph_of_address(df, address, time_zone)

if __name__ == "__main__":
    main()
