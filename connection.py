import datetime
from py5paisa import FivePaisaClient
import json
import websocket
import pandas as pd
import ast
from get_strike import get_client, get_strikes_code,get_message

alist = []
CALL_LIST  = []
import warnings
import requests

warnings.filterwarnings('ignore')
import time

BOT_TOKEN = "2042514901:AAHZkg3NyCMix_ThkEM5Tn6b4QX-h3QCoB0"
BOT_ID = "-719591358"
BID = '-1001655543039'

def telegram_bot_sendtext(bot_message, bot_id=BOT_ID, bot_token=BOT_TOKEN):
    bot_token = bot_token
    bot_chatID = bot_id
    send_text = 'https://api.telegram.org/bot' + bot_token + '/sendMessage?chat_id=' + bot_chatID + '&parse_mode=Markdown&text=' + bot_message
    response = requests.get(send_text)
    print(bot_message)
    return response.json()


def tele_message(m1):
    BOT_TOKEN2 = "2096544605:AAGOEKmWYKWLnjHJ53XJjrNvDedqLkD0qAM"
    BOT_ID2 = "-1001714579280"
    BOT_TOKEN1 = '2042514901:AAHZkg3NyCMix_ThkEM5Tn6b4QX-h3QCoB0'
    BOT_ID1 = '-1001664194044'
    telegram_bot_sendtext(m1)
    telegram_bot_sendtext(m1, bot_id=BOT_ID1, bot_token=BOT_TOKEN1)
    telegram_bot_sendtext(m1, bot_id=BOT_ID2, bot_token=BOT_TOKEN2)


def get_oi(st):
    st = ast.literal_eval(st)
    oi = st['OpenInterest']
    return st, oi


def get_req_list():
    df = pd.read_csv('option_script.csv')
    feature = ['Exch', 'ExchType', 'Scripcode']
    df1 = df[feature]
    adict = df1.to_dict('records')
    return adict, df


def firstrun_wecsocket(client: FivePaisaClient, wsPayload, df):
    client.web_url = f'wss://openfeed.5paisa.com/Feeds/api/chat?Value1={client.Jwt_token}|{client.client_code}'
    auth = client.Login_check()
    alist = []
    df['OI_Change'] = 0

    def on_message(ws, message):
        st, oi = get_oi(message)
        global alist
        if st['Token'] not in alist:
            df.loc[df['Scripcode'] == st['Token'], 'OI_Change'] = oi
            alist.append(st['Token'])
            if len(alist) >= len(df):
                alist = []
                print('run completed')
                df['OI_START'] = df['OI_Change']
                df.to_csv('opt_new.csv', index=False)
                df1 = pd.read_csv('opt_new.csv')
                df1 = df1[~(df1.OI == 0)]
                df1 = df1[~(df1.OI_Change == 0)]
                df1['Ch'] = round((df1.OI_Change - df1.OI) / (df1.OI) * 100, 2)
                df1['Ch_Diff'] = abs(df1.OI_Change - df1.OI)
                df1 = df1[df1.Ch_Diff > 5000]
                df1 = df1.sort_values('Ch', ascending=True, ignore_index=True)
                hr = datetime.datetime.now().hour
                min = datetime.datetime.now().minute
                df1.to_csv(f'datasets/ST_{hr}_{min}.csv', index=False)
                try:
                    for i in range(3):
                        stock = df1.Symbol.iloc[i]
                        strike = df1.StrikePrice.iloc[i]
                        option = df1.OptonType.iloc[i]
                        m1 = f'{stock}  {strike}{option} {df1.Ch.iloc[i]}% -{df1.Ch_Diff.iloc[i]}'
                        tele_message(m1)
                        time.sleep(1)
                    ws.close()
                except:
                    ws.close()

    def on_error(ws, error):
        print(error)

    def on_close(ws):
        print("Streaming Stopped")
        ws.close()

    def on_open(ws):
        print("Streaming Started")
        ws.send(json.dumps(wsPayload))

    ws = websocket.WebSocketApp(client.web_url,
                                on_open=on_open,
                                on_message=on_message,
                                on_error=on_error,
                                on_close=on_close,
                                cookie=auth)

    ws.run_forever()


def get_websocket_pre(client: FivePaisaClient, wsPayload, df, col='OI'):
    client.web_url = f'wss://openfeed.5paisa.com/Feeds/api/chat?Value1={client.Jwt_token}|{client.client_code}'
    auth = client.Login_check()
    alist = []
    df[col] = 0

    def on_message(ws, message):
        st, oi = get_oi(message)
        global alist
        if st['Token'] not in alist:
            df.loc[df['Scripcode'] == st['Token'], col] = oi
            alist.append(st['Token'])
            if len(alist) >= len(df):
                alist = []
                print('run completed')
                df.to_csv('opt_new.csv', index=False)
                ws.close()

    def on_error(ws, error):
        print(error)

    def on_close(ws):
        print("Streaming Stopped")
        ws.close()

    def on_open(ws):
        print("Streaming Started")
        ws.send(json.dumps(wsPayload))

    ws = websocket.WebSocketApp(client.web_url,
                                on_open=on_open,
                                on_message=on_message,
                                on_error=on_error,
                                on_close=on_close,
                                cookie=auth)

    ws.run_forever()


def get_websocket(client: FivePaisaClient, wsPayload, df, col='OI_Change'):
    client.web_url = f'wss://openfeed.5paisa.com/Feeds/api/chat?Value1={client.Jwt_token}|{client.client_code}'
    auth = client.Login_check()
    alist = []
    df[col] = 0

    def on_message(ws, message):
        st, oi = get_oi(message)
        global alist ,CALL_LIST
        if st['Token'] not in alist:
            df.loc[df['Scripcode'] == st['Token'], col] = oi
            alist.append(st['Token'])
            if len(alist) >= len(df):
                alist = []
                print('run completed')
                df.to_csv('opt_new.csv', index=False)
                df1 = pd.read_csv('opt_new.csv')
                df1['Ch'] = round((df1.OI_Change - df1.OI) / (df1.OI) * 100, 2)
                df1['Ch_Diff'] = abs(df1.OI_Change - df1.OI)
                df1 = df1.sort_values('Ch', ascending=True, ignore_index=True)
                hr = datetime.datetime.now().hour
                min = datetime.datetime.now().minute
                l1 = get_message(df1)
                for item in l1:
                    if item[1] not in CALL_LIST:
                        CALL_LIST.append(item[1])
                        tele_message(item[0])
                        m1 = ''.join(item[0].split(' ')[:3])
                        telegram_bot_sendtext(m1, bot_id=BID, bot_token=BOT_TOKEN)
                df1.to_csv(f'datasets/ST_{hr}_{min}.csv', index=False)
                print('run_completed')
                # tele_message('~~~~~ $$$$$$----- \n FINMAN TRADES - WAIT FOR THE CANDLE  \n TO CLOSE ABOVE LAST CANDLE \n ----$$$$$$$$ ~~~~~')
                df['OI_Prev'] = df['OI_Change']
                df.to_csv('opt_new.csv', index=False)
                ws.close()

    def on_error(ws, error):
        print(error)

    def on_close(ws):
        print("Streaming Stopped")
        ws.close()

    def on_open(ws):
        print("Streaming Started")
        ws.send(json.dumps(wsPayload))

    ws = websocket.WebSocketApp(client.web_url,
                                on_open=on_open,
                                on_message=on_message,
                                on_error=on_error,
                                on_close=on_close,
                                cookie=auth)

    ws.run_forever()


def pre_run(client):
    req_list, df = get_req_list()
    time.sleep(1)
    req_data = client.Request_Feed('oi', 's', req_list)
    get_websocket_pre(client, req_data, df)


def main_run(client):
    run = 0
    while datetime.time(9, 16) > datetime.datetime.now().time():
        time.sleep(1)
    req_list, df1 = get_req_list()
    time.sleep(1)
    df = pd.read_csv('opt_new.csv')
    req_data = client.Request_Feed('oi', 's', req_list)
    firstrun_wecsocket(client, req_data, df)
    print('hi')
    while datetime.time(9, 16) < datetime.datetime.now().time() < datetime.time(9, 20):
        if datetime.datetime.now().minute == 18:
            client = get_client()
            req_list, df1 = get_req_list()
            df = pd.read_csv('opt_new.csv')
            req_data = client.Request_Feed('oi', 's', req_list)
            get_websocket(client, req_data, df)
            time.sleep(60)
    print('hihi')
    while datetime.time(9, 20) < datetime.datetime.now().time() < datetime.time(15, 26):
        if datetime.datetime.now().minute % 3 == 0:
            client = get_client()
            req_list, df1 = get_req_list()
            time.sleep(1)
            df = pd.read_csv('opt_new.csv')
            req_data = client.Request_Feed('oi', 's', req_list)
            get_websocket(client, req_data, df, )
            time.sleep(60)
        time.sleep(1)
    print('hihihi')


def main():
    now = datetime.datetime.now().strftime('%A')
    L1 = ['26-Jan-2022', '19-Feb-2022', '01-Mar-2022', '18-Mar-2022',
          '01-Apr-2022', '02-Apr-2022', '10-Apr-2022', '14-Apr-2022',
          '15-Apr-2022', '01-May-2022', '03-May-2022', '16-May-2022',
          '10-Jul-2022', '09-Aug-2022', '15-Aug-2022', '14-Aug-2022',
          '31-Aug-2022', '02-Oct-2022', '05-Oct-2022', '09-Oct-2022',
          '24-Oct-2022', '26-Oct-2022', '08-Nov-2022', '25-Dec-2022']
    L1 = [datetime.datetime.strptime(x, "%d-%b-%Y").date() for x in L1]
    today = (datetime.datetime.today().date())
    if now in ['Sunday', 'Saturday']:
        tele_message(f'NO TRADING BECOZ OF SUNDAY')
    elif today in L1:
        tele_message(f'Today is Holiday')
    else:
        get_strikes_code()
        client = get_client()
        pre_run(client)
        main_run(client)


if __name__ == '__main__':
    main()
