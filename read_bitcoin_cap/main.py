import pyshark
import pymysql.cursors
from hashlib import *
from base58 import *
import subprocess
import base58check
import logging
from logging import getLogger, StreamHandler, Formatter, FileHandler
import re
import glob
import shutil
import time
import os
from multiprocessing import Process



class Log:
    def __init__(self, APPLICATIONNAME = "application"):
        #### logging setting ####
        self.APPLICATIONNAME = APPLICATIONNAME
        self.logLevel = logging.INFO
        self.logger = getLogger(self.APPLICATIONNAME)
        self.logger.setLevel(self.logLevel)
        self.handler_format = Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        self.streamSetting()
        self.filehandlerSetting()

    def streamSetting(self):
        #### logging streamhandler ####
        self.stream_handler = StreamHandler()
        self.stream_handler.setLevel(self.logLevel)
        self.stream_handler.setFormatter(self.handler_format)
        self.logger.addHandler(self.stream_handler)

    def filehandlerSetting(self):
        #### logging filehandler ####
        self.file_handler = FileHandler('./log/'+self.APPLICATIONNAME+'.log')
        self.file_handler.setLevel(self.logLevel)
        self.file_handler.setFormatter(self.handler_format)
        self.logger.addHandler(self.file_handler)

    def debugLog(self, *text):
        self.logger.debug(''.join([str(n) for n in text]))

    def infoLog(self, *text):
        self.logger.info(''.join([str(n) for n in text]))

    def warningLog(self, *text):
        self.logger.warning(''.join([str(n) for n in text]))

    def errorLog(self, *text):
        self.logger.error(''.join([str(n) for n in text]))

    def criticalLog(self, *text):
        self.logger.critical(''.join([str(n) for n in text]))

localLog = Log("readBitcoinPacket")

def SHA256D(bstr):
    localLog.debugLog("start SHA256D")
    return sha256(sha256(bstr).digest()).digest()

def ConvertPKHToAddress(prefix, addr):
    localLog.debugLog("start ConvertPKHToAddress")
    data = prefix + addr
    return b58encode(data + SHA256D(data)[:4]).decode('utf-8')

def PubkeyToAddress_P2PKH(pubkey_hex):
    localLog.debugLog("start PubkeyToAddress_P2PKH")
    try:
        pubkey = bytearray.fromhex(pubkey_hex)
    except:
        return pubkey_hex
    else:
        round1 = sha256(pubkey).digest()
        h = new('ripemd160')
        h.update(round1)
        pubkey_hash = h.digest()
        return ConvertPKHToAddress(b'\x00', pubkey_hash)

def PubkeyToAddress_P2SH(pubkey_hex):
    localLog.debugLog("start PubkeyToAddress_P2SH")
    try:
        pubkey = bytearray.fromhex(pubkey_hex)
    except:
        return pubkey_hex
    else:
        round1 = sha256(pubkey).digest()
        h = new('ripemd160')
        h.update(round1)
        pubkey_hash = h.digest()
        return ConvertPKHToAddress(b'\x05', pubkey_hash)

def ScriptToAddress(script_hex):
    localLog.debugLog("start ScriptToAddress")
    script = bytearray.fromhex(script_hex)
    round1 = SHA256D(script).hex()
    round2 = bytearray.fromhex(script_hex+round1[:8])
    return b58encode(bytes(round2)).decode('utf-8')

def GetOpecodeFromScript(script):
    localLog.debugLog("start GetOpecodeFromScript")
    try:
         opcode = subprocess.check_output(["bx", "script-decode", script]).decode().replace("\n","")
    except:
         return None
    return opcode.split(" ")


def GetVersionAndPublicKeyFromBitcoinScript(script):
    localLog.debugLog("start GetVersionAndPublicKeyFromBitcoinScript")
    opcode_list = GetOpecodeFromScript(script)
    if opcode_list == None:
        return None, None
    elif 'dup' == opcode_list[0] and 'hash160' == opcode_list[1]:
        return 1 ,opcode_list[2].replace("[","").replace("]","")
    elif 'hash160' == opcode_list[0] and 'equal' == opcode_list[2]:
        return 5, opcode_list[1].replace("[","").replace("]","")
    elif 'return' == opcode_list[0]:
        return None, None
    elif '<invalid>' == opcode_list[-1]:
        return None, None
    else:
        return None, opcode_list

def GetVersionAndPublicKeyFromBitcoinSignature(sig_script):
    localLog.debugLog("start GetVersionAndPublicKeyFromBitcoinSignature")
    sig_list = GetOpecodeFromScript(sig_script)
    if sig_list == None:
        return None, None
    elif sig_list[-1] == "<invalid>":
        return None, None
    elif (sig_list[0] == '2' or sig_list[0] == '1') and sig_list[-1] == 'checkmultisig':
        return 5, sig_script
    elif len(sig_list) == 1:
        if '[0014' == sig_list[0][:5]:
            return 5, sig_list[0].replace("[","").replace("]","")
        else:
            return None, None
    elif sig_list[0] == "zero":
        return  None, sig_list[1:]
    else:
        return 1, sig_list[1].replace("[","").replace("]","")

def GetAddressFromPublicKeyByVersionInOutput(version, public_key):
    localLog.debugLog("start GetAddressFromPublicKeyByVersionInOutput")
    if version == 1:
        return ScriptToAddress("00"+public_key)
    elif version == 5:
        return ConvertPKHToAddress(b'\x05', bytearray.fromhex(public_key))

def GetAddressFromPublicKeyByVersionInInput(version, public_key):
    localLog.debugLog("start GetAddressFromPublicKeyByVersionInInput")
    if version == 1:
        return PubkeyToAddress_P2PKH(public_key)
    elif version == 5:
        return PubkeyToAddress_P2SH(public_key)

def GetInputDataFromPacket(packet):
    localLog.debugLog("start GetInputDataFromPacket")
    addresses = []
    if packet["BITCOIN"].get_field_value('command') == "tx" and packet["BITCOIN"].get_field_value('tx').get_field_value('input_count') != "0":
        input_count = int(packet["BITCOIN"].get_field_value('tx').get_field_value('input_count'))
        input_list = packet["BITCOIN"].get_field_value('tx').get_field_value('in')
        if input_count > 1:
            for input_num in range(input_count):
                sig_script = input_list[input_num].get_field_value("sig_script").replace(':','')
                version, public_keys = GetVersionAndPublicKeyFromBitcoinSignature(sig_script)
                if type(public_keys) is list:
                    for sig_script in public_keys:
                        version, public_key = GetVersionAndPublicKeyFromBitcoinSignature(sig_script.replace("[","").replace("]",""))
                        if public_key != None:
                            addresses.append(GetAddressFromPublicKeyByVersionInInput(version, public_key))

                elif public_keys != None:
                        addresses.append(GetAddressFromPublicKeyByVersionInInput(version, public_keys))

        elif input_count == 1:
            sig_script = input_list.get_field_value("sig_script").replace(':','')
            if sig_script != "None":
                version, public_keys = GetVersionAndPublicKeyFromBitcoinSignature(sig_script)
                if type(public_keys) is list:
                    for sig_script in public_keys:
                        version, public_key = GetVersionAndPublicKeyFromBitcoinSignature(sig_script.replace("[","").replace("]",""))
                        if public_key != None:
                            addresses.append(GetAddressFromPublicKeyByVersionInInput(version, public_key))

                elif public_keys != None:
                        addresses.append(GetAddressFromPublicKeyByVersionInInput(version, public_keys))
    return list(set(addresses))

def GetOutputDataFromPacket(packet):
    localLog.debugLog("start GetOutputDataFromPacket")
    addresses = []
    if packet["BITCOIN"].get_field_value('command') == "tx" and packet["BITCOIN"].get_field_value('tx').get_field_value('output_count') != "0":
        output_count = int(packet["BITCOIN"].get_field_value('tx').get_field_value('output_count'))
        output_list = packet["BITCOIN"].get_field_value('tx').get_field_value('out')
        if output_count > 1:
            for output_num in range(output_count):
                if output_list[output_num].has_field("script"):
                    script = output_list[output_num].get_field_value("script").replace(':','')
                    version, public_key = GetVersionAndPublicKeyFromBitcoinScript(script)
                    if public_key != None:
                        addresses.append(GetAddressFromPublicKeyByVersionInOutput(version, public_key))
        else:
            if output_list.has_field("script"):
                script = output_list.get_field_value("script").replace(':','')
                version, public_key = GetVersionAndPublicKeyFromBitcoinScript(script)
                if public_key != None:
                    addresses.append(GetAddressFromPublicKeyByVersionInOutput(version, public_key))
    return addresses


def GetSrtIpFromPacket(packet):
    localLog.debugLog("start GetSrtIpFromPacket")
    if packet["IP"].get_field_value('src_host'):
        return packet["IP"].get_field_value('src_host')
    else:
        return "0.0.0.0"

def GetDstIpFromPacket(packet):
    localLog.debugLog("start GetDstIpFromPacket")
    if packet["IP"].get_field_value('dst'):
        return packet["IP"].get_field_value('dst')
    else:
        return "0.0.0.0"

def GetTimeFromPacket(packet):
    localLog.debugLog("start GetTimeFromPacket")
    return packet.sniff_timestamp

def GetTXID(packet):
    localLog.debugLog("start GetTXID")
    if packet["BITCOIN"].get_field_value('command') == "tx" and packet["BITCOIN"].get_field_value('tx').get_field_value('input_count') != "0":
        inputs = packet["BITCOIN"].get_field_value('tx').get_field_value('in')
        tx_raw = packet["BITCOIN"].get_field_value('tx_raw')[0]
        txid = SHA256D(bytearray.fromhex(tx_raw)).hex()
        txid_reversed = "".join(reversed(re.split('(..)',txid)[1::2]))
        return txid_reversed
    else:
        return None

def GetBitcoinData(file_path):
    localLog.debugLog("start GetBitcoinData")
    cap = pyshark.FileCapture(file_path, include_raw=True, use_json=True, keep_packets=False)
    tx_st = []
    tx_address_st = []
    num = 0
    for packet in cap:
        if "BITCOIN" in packet:
            txid = GetTXID(packet)
            if txid != None:
                if num == 10:
                    break
                num += 1
                src_ip = GetSrtIpFromPacket(packet)
                dst_ip = GetDstIpFromPacket(packet)
                time = GetTimeFromPacket(packet)
                addresses = GetInputDataFromPacket(packet)
                for address in addresses:
                    tx_address = {}
                    tx_address["TX_ID"] = txid
                    tx_address["ADDRESS"] = address
                    tx_address["IN_OUT_FLAG"] = "0"
                    tx_address_st.append(tx_address)

                addresses = GetOutputDataFromPacket(packet)
                for address in addresses:
                    tx_address = {}
                    tx_address["TX_ID"] = txid
                    tx_address["ADDRESS"] = address
                    tx_address["IN_OUT_FLAG"] = "1"
                    tx_address_st.append(tx_address)
                tx = {}
                tx["TX_ID"] = txid
                tx["TIME"] = time
                tx["SRC_IP"] = src_ip
                tx["DST_IP"] = dst_ip
                tx_st.append(tx)
    return tx_st, tx_address_st

def InsertTx(tx_st):
    localLog.debugLog("start InsertTxAddress")
    sql = "INSERT INTO TX (TX_ID, TIME, SRC_IP, DST_IP) VALUES"
    con = pymysql.connect(host='127.0.0.1',user='root',password='',db='Bitcoin',charset='utf8',cursorclass=pymysql.cursors.DictCursor)
    cur = con.cursor()
    for tx in tx_st:
        sql += ' ("'
        sql += tx["TX_ID"] + '", "'
        sql += tx["TIME"] + '", "'
        sql += tx["SRC_IP"] + '", "'
        sql += tx["DST_IP"] + '"),'
    sql = sql[:-1]
    try:
        r = cur.execute(sql)
    except Exception as e:
        localLog.warningLog(e)
        localLog.warningLog(sql)
    con.commit()
    con.close()

def InsertTxAddress(tx_address_st):
    localLog.debugLog("start InsertTxAddress")
    sql = "INSERT INTO TX_ADDRESS (TX_ID, ADDRESS, IN_OUT_FLAG) VALUES"
    con = pymysql.connect(host='127.0.0.1',user='root',password='',db='Bitcoin',charset='utf8',cursorclass=pymysql.cursors.DictCursor)
    cur = con.cursor()
    for tx_address in tx_address_st:
        sql += ' ("'
        sql += tx_address["TX_ID"] + '", "'
        sql += tx_address["ADDRESS"] + '", "'
        sql += tx_address["IN_OUT_FLAG"] + '"),'
    sql = sql[:-1]
    try:
        r = cur.execute(sql)
    except Exception as e:
        localLog.warningLog(e)
        localLog.warningLog(sql)
    con.commit()
    con.close()

def ProcessCapFile(file_path):
    new_file_path = shutil.move(file_path, './data')
    localLog.infoLog("start to process : ", new_file_path)
    tx_st, tx_address_st = GetBitcoinData(new_file_path)
    InsertTx(tx_st)
    InsertTxAddress(tx_address_st)
    os.remove(new_file_path)
    localLog.infoLog("finish to process : ", new_file_path)

def main():
    # tshark -i en0 -b duration:10 -w bitcoin01
    cap_dir_path = '../../../03,データセット/*.cap'
    while True:
        file_path_list = glob.glob(cap_dir_path)
        if file_path_list == []:
            localLog.infoLog("file is nothing.")
            time.sleep(10)
        process_list = []
        for file_path in file_path_list:
            process_list.append(Process(target=ProcessCapFile, args=(file_path,)))
        for process_num in range(len(process_list)):
                process_list[process_num].start()
        for process_num in range(len(process_list)):
                process_list[process_num].join()



if __name__ == '__main__':
    main()
