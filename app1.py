import numpy as np  # np mean, np random
import pandas as pd  # read csv, df manipulation
#import plotly.express as px  # interactive charts
import streamlit as st  # 🎈 data web app development
import datetime as dt

st.set_page_config(page_title='Option Chain',page_icon='random',layout='wide')

def get_message(df):
    df5 = df[:10]
    # df2 = df1[df1.Symbol.duplicated()]
    symbols = df5.Symbol.unique()
    l1 = []
    for i in symbols:
        df1 = df[df.Symbol==i]
        # df1 = df1[(df1.Ch_Diff)>5000]
        df1.loc[df1['Ch']<=0,'SIGNAL'] = True
        df1.loc[df1['Ch'] > 0, 'SIGNAL'] = False
        df2 = df1[df1.OptonType =='CE']
        df3 = df1[df1.OptonType =='PE']
        df2 = df2.reset_index()
        df3 = df3.reset_index()
        ce =all(list(df2.SIGNAL))
        pe = all(list(df3.SIGNAL))
        if ce:
            l1.append(df2[:1])
        if pe:
            l1.append(df3[:1])

    if len(l1)==0:
        return pd.DataFrame(columns=['Name', 'Symbol', 'Ch', 'Ch_Diff'])
    else:
        df = pd.concat(l1, ignore_index=True)
        return df


def get_time_slots():
    b = []
    h = 9
    m = 18
    for i in range(123):
        if m == 60:
            h = h + 1
            m = 0
        a = dt.time(h, m)
        m = m + 3
        b.append(a)
    return b

sequence = np.array(get_time_slots())

st.title('Stock Selection On % OI')

t = st.select_slider('SELECT TIME',options=sequence,value= dt.time(9, 18))
hr = t.hour
min = t.minute
if dt.time(9,15)<dt.datetime.now().time() < dt.time(15,30):
    hr = dt.datetime.now().hour
    min = int(dt.datetime.now().minute /3)*3
    if min==0:
        hr1 = hr-1
        min1 = 57
        min2 = 54
        hr2 = hr-1
    elif min ==3:
        hr1 = hr
        hr2 = hr-1
        min1 = 0
        min2 = 57
    else:
        hr1,hr2,min1,min2 = hr,hr,min-3,min-6
    col1, col2 = st.columns(2)
    col1.header(f"{hr}.{min} UNWINDING")
    df1 = pd.read_csv(f'datasets/ST_{hr}_{min}.csv')
    col = ['Name','Symbol','Ch','Ch_Diff']
    df_1 = get_message(df1)
    df1 =df1[col]
    col1.dataframe(df1[:3],height = 200)
    col2.header(f"{hr}.{min} Double")
    df_1 =df_1[col]
    col2.dataframe(df_1,height = 200)
    col1, col2 = st.columns(2)
    col1.header(f"{hr1}.{min1} UNWINDING")
    df1 = pd.read_csv(f'datasets/ST_{hr1}_{min1}.csv')
    col = ['Name', 'Symbol', 'Ch', 'Ch_Diff']
    df_1 = get_message(df1)
    df1 = df1[col]
    col1.dataframe(df1[:3], height=200)
    col2.header(f"{hr1}.{min1} ")
    df_1 = df_1[col]
    col2.dataframe(df_1, height=200)
    col1, col2 = st.columns(2)
    col1.header(f"{hr2}.{min2} UNWINDING")
    df1 = pd.read_csv(f'datasets/ST_{hr2}_{min2}.csv')
    col = ['Name', 'Symbol', 'Ch', 'Ch_Diff']
    df_1 = get_message(df1)
    df1 = df1[col]
    col1.dataframe(df1[:3], height=200)
    col2.header(f"{hr2}.{min2} Double")
    df_1 = df_1[col]
    col2.dataframe(df_1, height=200)
else:
    col1, col2 = st.columns(2)
    col1.header(f"{hr}.{min} UNWINDING")
    df1 = pd.read_csv(f'datasets/ST_{hr}_{min}.csv')
    col = ['Name','Symbol','Ch','Ch_Diff']
    df_1 = get_message(df1)
    df1 =df1[col]
    col1.dataframe(df1[:3],height = 200)
    col2.header(f"{hr}.{min} Double")
    df_1 =df_1[col]
    col2.dataframe(df_1,height = 200)
    col1, col2 = st.columns(2)
    col1.header("9.21")
    df1 = pd.read_csv(f'datasets/ST_9_21.csv')
    col = ['Name', 'Symbol', 'Ch', 'Ch_Diff']
    df_1 = get_message(df1)
    df1 = df1[col]
    col1.dataframe(df1[:3], height=200)
    col2.header("9.21 Double")
    df_1 = df_1[col]
    col2.dataframe(df_1, height=200)

    col1, col2 = st.columns(2)
    col1.header("9.24")
    df1 = pd.read_csv(f'datasets/ST_9_27.csv')
    col = ['Name', 'Symbol', 'Ch', 'Ch_Diff']
    df_1 = get_message(df1)
    df1 = df1[col]
    col1.dataframe(df1[:3], height=200)
    col2.header("9.24 Double")
    df_1 = df_1[col]
    col2.dataframe(df_1, height=200)


