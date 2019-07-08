import pyshark

from hashlib import *
from base58 import *

file_path = '/Users/igakishuusei/Desktop/bitcoin.cap'
file_path = '../../../03,データセット/bitcoin.cap'
num_of_read_cap = 1000




def SHA256D(bstr):
    return sha256(sha256(bstr).digest()).digest()

def ConvertPKHToAddress(prefix, addr):
    data = prefix + addr
    return b58encode(data + SHA256D(data)[:4])

def PubkeyToAddress(pubkey_hex):
    try:
        pubkey = bytearray.fromhex(pubkey_hex)
    except:
        print(pubkey_hex)
    round1 = sha256(pubkey).digest()
    h = new('ripemd160')
    h.update(round1)
    pubkey_hash = h.digest()
    return ConvertPKHToAddress(b'\x00', pubkey_hash)

def ScriptToAddress(script_hex):
    script = bytearray.fromhex(script_hex)
    round1 = SHA256D(script).hex()
    round2 = bytearray.fromhex(script_hex+round1[:8])
    return b58encode(bytes(round2))

def GetInputDataFromPacket(packet):
    if packet["BITCOIN"].get_field_value('command') == "tx" and packet["BITCOIN"].get_field_value('tx').get_field_value('input_count') != "0":
        print("- intput")
        input_count = int(packet["BITCOIN"].get_field_value('tx').get_field_value('input_count'))
        input_list = packet["BITCOIN"].get_field_value('tx').get_field_value('in')

        if input_count > 1:
            for num in range(input_count):
                script = input_list[num].get_field_value("sig_script").replace(':','')
                print(PubkeyToAddress(script[2:144]))
        elif input_count == 0:
            pass
        else:
            script = input_list.get_field_value("sig_script").replace(':','')
            if script != "None":
                print(PubkeyToAddress(script[2:144]))

def GetOutputDataFromPacket(packet):
    if packet["BITCOIN"].get_field_value('command') == "tx" and packet["BITCOIN"].get_field_value('tx').get_field_value('output_count') != "0":
        print("- output")
        print(packet["BITCOIN"].get_field_value('tx'))
        input_count = int(packet["BITCOIN"].get_field_value('tx').get_field_value('input_count'))
        output_count = int(packet["BITCOIN"].get_field_value('tx').get_field_value('output_count'))
        output_list = packet["BITCOIN"].get_field_value('tx').get_field_value('out')
        if input_count != 0:
            if output_count > 1:
                for num in range(output_count):
                    if output_list[num].has_field("script"):
                        script = output_list[num].get_field_value("script").replace(':','')
                        print(ScriptToAddress("00"+script[6:46]))
            else:
                if output_list.has_field("script"):
                    script = output_list.get_field_value("script").replace(':','')
                    print(ScriptToAddress("00"+script[6:46]))

        else:
            if output_count > 1:
                for num in range(output_count):
                    if output_list[num].has_field("script"):
                        script = output_list[num].get_field_value("script").replace(':','')
                        print(ScriptToAddress("00"+script[6:46]))
            else:
                if output_list.has_field("script"):
                    script = output_list.get_field_value("script").replace(':','')
                    print(ScriptToAddress("00"+script[6:46]))

def GetIpFromPacket(packet):
    if packet["IP"].get_field_value('src_host'):
        print("src ip : "+packet["IP"].get_field_value('src_host'))
    if packet["IP"].get_field_value('dst'):
        print("dst ip : "+packet["IP"].get_field_value('dst'))

def GetTcpFromPacket(packet):
    print(packet.sniff_timestamp)

def main():
    cap = pyshark.FileCapture(file_path, include_raw=True, use_json=True)
    num = 0
    for packet in cap:
        if "BITCOIN" in packet:
            print("====================================")
            print("num of packet : "+str(num))
            num += 1
            GetIpFromPacket(packet)
            GetTcpFromPacket(packet)
            GetInputDataFromPacket(packet)
            GetOutputDataFromPacket(packet)


if __name__ == '__main__':
    main()
