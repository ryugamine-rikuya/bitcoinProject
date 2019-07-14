export BITCOIN_CAP_FILE_PATH=/Users/igakishuusei/Google/01,University/02,4年生/03,ゼミ/01,研究関連/04,プログラム/bitcoinProject/read_bitcoin_cap/data/bitcoin
echo $BITCOIN_CAP_FILE_PATH
nohup tshark -i en0 -b duration:10 -w $BITCOIN_CAP_FILE_PATH &
