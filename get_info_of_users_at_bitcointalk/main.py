import requests
import pymysql.cursors
from bs4 import BeautifulSoup
import time
import threading
import copy

BASE_URL = "https://bitcointalk.org/index.php?action=profile;u={}"
SQL = "INSERT INTO Users (user_id, name, posts, activity, merit, position, dateregistered, lastactive, icq, aim, msn, yim, email, website, currentstatus, bitcoinaddress, Gender, age, location, locationtime, customtitle) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
THREAD_NUM = 4
USER_INFO_LIST = []
lock = threading.Lock()

def get_info_of_users_at_bitcointalk(start_number, user_number):
    connection = pymysql.connect(host='127.0.0.1',
                                 user='root',
                                 password='',
                                 db='Bitcoin',
                                 charset='utf8',
                                 cursorclass=pymysql.cursors.DictCursor)
    for number in range(start_number,user_number):
    	print(number)
    	user_info = {
    		"Id":number,
    		"Name":"",
    		"Posts":"",
    		"Activity":"",
    		"Merit":"",
    		"Position":"",
    		"Date Registered":"",
    		"Last Active":"",
    		"ICQ":"",
    		"AIM":"",
    		"MSN":"",
    		"YIM":"",
    		"Email":"",
    		"Website":"",
    		"Current Status":"",
    		"Bitcoin address":"",
    		"Gender":"",
    		"Age":"",
    		"Location":"",
    		"Local Time":"",
    		"Custom Title":""
    	}
    	target_url = BASE_URL.format(number)
    	while True:
    		try:
    			r = requests.get(target_url, timeout=4)
    		except:
    			time.sleep(2)
    		else:
    			if (r.status_code == requests.codes.ok):
    				print(r.status_code)
    				break
    			else:
    				print(r.status_code)

    	soup = BeautifulSoup(r.text, 'lxml')
    	tables = soup.findAll("table")
    	if len(tables) ==  10:
    		trs = tables[5].findAll("tr")[0].findAll("tr")[1].findAll("tr")
    		num = 0
    		for tr in trs:
    			if len(tr.findAll("hr")) == 0:
    				if len(tr.findAll("b")) != 0:
    					key = tr.findAll("b")[0].text.split(":")[0]
    					if  key in user_info:
    						value = tr.findAll("td")[1].text
    						user_info[key] = value
    		bitcoin_address = user_info["Bitcoin address"].replace("   ","//")
    		bitcoin_address = bitcoin_address.replace("  ","///")
    		bitcoin_address = bitcoin_address.replace("\n","")
    		with connection.cursor() as cursor:
    		    r = cursor.execute(SQL, (user_info["Id"],user_info["Name"],user_info["Posts"],user_info["Activity"],user_info["Merit"],user_info["Position"],user_info["Date Registered"],user_info["Last Active"],user_info["ICQ"],user_info["AIM"],user_info["MSN"],user_info["YIM"],user_info["Email"],user_info["Website"],user_info["Current Status"],bitcoin_address,user_info["Gender"],user_info["Age"],user_info["Location"],user_info["Local Time"],user_info["Custom Title"]))
    		connection.commit()
    connection.close()

def get_info_of_users_at_bitcointalk_tmp(thread_number, start_number, user_number):
    print("{} {} {}".format(thread_number, start_number, user_number))
    for number in range(start_number,user_number):
    	print("thread_num : {}, user_id : {}".format(thread_number, number))
    	user_info = {
    		"Id":number,
    		"Name":"",
    		"Posts":"",
    		"Activity":"",
    		"Merit":"",
    		"Position":"",
    		"Date Registered":"",
    		"Last Active":"",
    		"ICQ":"",
    		"AIM":"",
    		"MSN":"",
    		"YIM":"",
    		"Email":"",
    		"Website":"",
    		"Current Status":"",
    		"Bitcoin address":"",
    		"Gender":"",
    		"Age":"",
    		"Location":"",
    		"Local Time":"",
    		"Custom Title":""
    	}
    	target_url = BASE_URL.format(number)
    	while True:
    		try:
    			r = requests.get(target_url, timeout=4)
    			print(r.status_code)
    		except:
    			print("thread_num : {}, error".format(thread_number))
    			time.sleep(2)
    		else:
    			break
    	print(r.status_code)
    	soup = BeautifulSoup(r.text, 'lxml')
    	tables = soup.findAll("table")
    	if len(tables) ==  10:
    		trs = tables[5].findAll("tr")[0].findAll("tr")[1].findAll("tr")
    		num = 0
    		for tr in trs:
    			if len(tr.findAll("hr")) == 0:
    				if len(tr.findAll("b")) != 0:
    					key = tr.findAll("b")[0].text.split(":")[0]
    					if  key in user_info:
    						value = tr.findAll("td")[1].text
    						user_info[key] = value
    		bitcoin_address = user_info["Bitcoin address"].replace("   ","//")
    		bitcoin_address = bitcoin_address.replace("  ","///")
    		user_info["Bitcoin address"] = bitcoin_address.replace("\n","")
    		print("==================")
    		print(user_info)
    		global USER_INFO_LIST
    		global lock
    		lock.acquire()
    		USER_INFO_LIST.append(user_info)
    		lock.release()

def insert_user_info():
    connection = pymysql.connect(host='127.0.0.1',
                                 user='root',
                                 password='',
                                 db='Bitcoin2019',
                                 charset='utf8',
                                 cursorclass=pymysql.cursors.DictCursor)
    cursor = connection.cursor()
    for i in range(10):
        global USER_INFO_LIST
        global lock
        lock.acquire()
        user_info_list_local = copy.copy(USER_INFO_LIST)
        USER_INFO_LIST = []
        lock.release()
        for user_info in user_info_list_local:

            print(user_info)
            try:
                r = cursor.execute(SQL, (user_info["Id"],user_info["Name"],user_info["Posts"],user_info["Activity"],user_info["Merit"],user_info["Position"],user_info["Date Registered"],user_info["Last Active"],user_info["ICQ"],user_info["AIM"],user_info["MSN"],user_info["YIM"],user_info["Email"],user_info["Website"],user_info["Current Status"],user_info["Bitcoin address"],user_info["Gender"],user_info["Age"],user_info["Location"],user_info["Local Time"],user_info["Custom Title"]))
            except MySQLdb.Error as e:
                print('MySQLdb.Error: ', e)
            else:
                connection.commit()

        time.sleep(1)
    connection.close()


def main():
    start_number= 2258182
    #start_number= 2423987
    user_number = 2637581

    thread_list = []
    #n = int((user_number - start_number) / THREAD_NUM)
    get_info_of_users_at_bitcointalk(start_number, user_number)
    """
    for num in range(THREAD_NUM):
        thread_list.append(threading.Thread(target=get_info_of_users_at_bitcointalk, kwargs={'thread_number': num, 'start_number': start_number + n * num, 'user_number': start_number + n * (num+1)}))
    thread_list.append(threading.Thread(target=insert_user_info))
    for num in range(THREAD_NUM + 1):
        thread_list[num].start()
    """

if __name__ == '__main__':
    main()



# create table
#create table bitcoin_toalk.Users(id int not null primary key AUTO_INCREMENT,user_id int ,name varchar(255), posts varchar(255), activity varchar(255), merit varchar(255), position varchar(255), dateregistered varchar(255), lastactive varchar(255), icq varchar(255), aim varchar(255), msn varchar(255), yim varchar(255), email varchar(255), website varchar(255), currentstatus varchar(255), bitcoinaddress varchar(255), Gender varchar(255), age varchar(255), location varchar(255), locationtime varchar(255), customtitle varchar(255));
