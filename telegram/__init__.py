import requests


def send(mess):
    chat_id = '-663828012'
    token = '5407215024:AAFSS85FiIxapcSRF-rYzx2KhFlIdAVhasg'
    telegram_msg = requests.get(f'https://api.telegram.org/bot{token}/sendMessage?chat_id={chat_id}&text={mess}')
    print(telegram_msg)
    # print(telegram_msg.content)