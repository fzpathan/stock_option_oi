import pandas as pd
from py5paisa import FivePaisaClient
import time
import warnings
warnings.filterwarnings('ignore')
def get_client():
    cred = {
        "APP_NAME": 
        "APP_SOURCE": 
        "USER_ID":
        "PASSWORD": 
        "USER_KEY": 
        "ENCRYPTION_KEY": 
    }

    email = 
    passwd = 
    dob = 
    client = FivePaisaClient(email=email, passwd=passwd, dob=dob,cred=cred)
    print(client)
    client.login()
    return client

def get_scrips(expiry):
    df = pd.read_csv('scripmaster-csv-format.csv')
    df = df[(df.Exch == 'N') & (df.ExchType == 'D') & (df.Expiry == f'{expiry} 14:30:00')]
    feature = ['Exch', 'ExchType', 'Scripcode', 'Name', 'Expiry','Root']
    df1 = df[feature]
    print(df1.head())
    df1['Symbol'] = df1.Name.str.split(' ',expand=True)[0]
    df1['StrikePrice'] = df1.Name.str.split(' ',expand=True)[5]
    df1['OptonType'] =  df1.Name.str.split(' ',expand=True)[4]
    df1.to_csv('instrument_list.csv',index=False)
    return df1

def get_ltp(client,symbol):
    req_list_ = [{"Exch": "N", "ExchType": "C", "Symbol": symbol}]
    a = client.fetch_market_feed(req_list_)
    ltp = a['Data'][0]['LastRate']
    time.sleep(1)
    return ltp

def get_ltp_df(df,client):
    list_symbol = list(df.Symbol.unique())
    df['LTP'] = 0
    for i in list_symbol:
        try:
            a = get_ltp(client,i)
            df.loc[df['Symbol'] == i, 'LTP'] = a
            print(i,a)
            
        except:
            pass
    df = df[df['LTP']>400]
    df.to_csv('instrument_list.csv',index=False)
    return df

def get_list(l1,ltp):
    if not isinstance(ltp,float):
        return [] , []
    l2 = [i for i in l1 if i>ltp]
    l3 = [i for i in l1 if i<=ltp]
    l2.sort()
    l3.sort()
    return l2,l3

def select_strike(df):
    list_dfs = []
    for i in df.Symbol.unique():
        try:
            df1 = df[df.Symbol==i]
            l1 = list(df1.StrikePrice.unique())
            l1 = [float(i) for i in l1]
            l1.sort()
            ltp = list(df1.LTP.unique())[0]
            l2,l3 = get_list(l1,ltp)
            a = l2[:2]
            b = l3[-2:]
            a.extend(b)
            df2 = df1[df1.StrikePrice.isin(a)]
            list_dfs.append(df2)
        except BaseException as e:
            print(i,e.args)
    data = pd.concat(list_dfs,ignore_index=True)
    print(data.head())
    data.to_csv('option_script.csv', index=False)

def get_strikes_code():
    expiry = '2022-08-25'
    get_scrips(expiry)
    df = pd.read_csv('instrument_list.csv')
    client = get_client()
    df = get_ltp_df(df,client)
    select_strike(df)

def get_message(df):
    df5 = df[:10]
    # df2 = df1[df1.Symbol.duplicated()]
    symbols = df5.Symbol.unique()
    l1 = []
    for i in symbols:
        df1 = df[df.Symbol==i]
        df1 = df1[~(df1.Ch==0)]
        # 
        df1.loc[df1['Ch']<=0,'SIGNAL'] = True
        df1.loc[df1['Ch'] > 0, 'SIGNAL'] = False
        df2 = df1[df1.OptonType =='CE']
        df3 = df1[df1.OptonType =='PE']
        df2 = df2.reset_index()
        df3 = df3.reset_index()
        ce =all(list(df2.SIGNAL))
        pe = all(list(df3.SIGNAL))

        if ce:
            stock = df3.Symbol.iloc[0]
            strike = df3.StrikePrice.iloc[0]
            m1 = f'{stock}  {strike} CE {df3.Ch.iloc[0]}% -{df3.Ch_Diff.iloc[0]}'
            if [m1,f'{stock}CE'] not in l1:
                l1.append([m1,f'{stock}CE'])
        if pe:
            stock = df2.Symbol.iloc[0]
            strike = df2.StrikePrice.iloc[0]
            m1 = f'{stock}  {strike} PE {df2.Ch.iloc[0]}% -{df2.Ch_Diff.iloc[0]}'
            if [m1,f'{stock}PE'] not in l1:
                l1.append([m1,f'{stock}PE'])
    return l1
