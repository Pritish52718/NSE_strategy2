#!/usr/bin/env python
# coding: utf-8

# In[61]:


#!/usr/bin/env python
# coding: utf-8

import pandas as pd
import streamlit as st
import requests, zipfile, io,logging
import shutil
import copy
import sys
import os
from datetime import datetime,date,timedelta
import pytz
from dateutil.relativedelta import relativedelta, TH

st.set_page_config(layout="wide")

nthu = datetime.today()
while (nthu + relativedelta(weekday=TH(2))).month == datetime.today().month:
    nthu += relativedelta(weekday=TH(2))

d_path="Data"
e_path="Other_Data"

def get_df(df):
    #df = pd.read_csv(d_path+'/'+name)    
    df.columns = df.columns.str.strip()
    df = df.map(lambda x: x.strip() if type(x)==str else x)
    df=df[['TradDt','FinInstrmTp','TckrSymb','XpryDt','StrkPric','OptnTp','OpnPric','HghPric','LwPric','ClsPric',
           'UndrlygPric','OpnIntrst','NewBrdLotQty','TtlTradgVol']]
    df.rename(columns={'TradDt':'TIMESTAMP','FinInstrmTp': 'INSTRUMENT', 'TckrSymb': 'SYMBOL', 'XpryDt': 'EXPIRY_DT','StrkPric': 'STRIKE_PR',
             'OptnTp': 'OPTION_TYP',
             'OpnPric': 'OPEN',
             'HghPric': 'HIGH',
             'LwPric': 'LOW',
             'ClsPric': 'CLOSE',
             'OpnIntrst': 'OPEN_INT',
             'UndrlygPric':'EQ_price',
            'NewBrdLotQty':'Lot_size',
            'TtlTradgVol':'CONTRACTS'
            },inplace=True)
    df.reset_index(drop=True,inplace=True)

    return df





def drop_y(df,filename):
    # list comprehension of the cols that end with '_y'
    to_drop = [x for x in df if x.endswith('_y')]
    if "LOW_y" in to_drop:
        to_drop.remove("LOW_y")
        to_drop.remove('CONTRACTS_y')
        to_drop.remove('OPEN_INT_y')
    if to_drop:
        df.drop(to_drop, axis=1, inplace=True)
        rename_x(df,filename)
def rename_x(df,filename):
    for col in df:
        if col.endswith('_x'):
            df.rename(columns={col:col.rstrip('_x')}, inplace=True)
        elif col.endswith('_y'):
            df.rename(columns={col:col.rstrip('_y')+'_'+filename},inplace=True)






today_date=datetime.now().strftime("%Y%m%d")
logging.basicConfig(filename="Log_"+today_date+".log", format='%(asctime)s %(message)s', filemode='w') 
logger=logging.getLogger() 
logger.setLevel(logging.INFO) 



#Populating today's date as default, if the stat_date and/or End_date is not provided.
@st.cache_data(ttl=21600)
def downld_data():
    
    dfns=pd.DataFrame()
    dfnf=pd.DataFrame()
    global No_of_download,Working_day,Non_Work_day
    #df_ns,df_nf=pd.DataFrame(),pd.DataFrame()
    No_of_download=0
    Working_day=0
    Non_Work_day=0
    Start_date=""
    End_date=""
    check=True

    if Start_date=="" or Start_date=="enter_start_date_in_DDMMMYYYY":
        Start_date=(datetime.now()-timedelta(days=14)).strftime("%Y%m%d")
        End_date=today_date
    if End_date=="" or End_date=="enter_start_date_in_DDMMMYYYY":
            End_date=today_date

    daterange = pd.date_range(datetime.strptime(Start_date, "%Y%m%d"),datetime.strptime(End_date, "%Y%m%d"))
    lis,skip_dates=[],[]
    #Looping through each date, and downloading the file.
    for single_date in daterange:
        loop_date=single_date.strftime("%Y-%m-%d")
        year,month,date=loop_date.split('-')
        month=month.upper()
        weekday=single_date.weekday()
        #If day is not Saturday or Sunday,then proceed to download the file.
        if weekday not in [5,6]:
            Working_day=Working_day+1
            logger.info("Trying to download File of :"+loop_date)
            temp_zip_file_url = "https://www.samco.in/bse_nse_mcx/getBhavcopy"
            logger.info(temp_zip_file_url)
            headers = {
                'Content-Type': 'application/x-www-form-urlencoded',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/85.0.4183.121 Safari/537.36'
        }
            payload = {
                    'start_date': '2024-10-14',
                    'end_date': '2024-10-16',
                    'show_or_down': '2',
                    'bhavcopy_data[]': 'NSEFO'  # Handle array-like parameter correctly
                }
            with requests.Session() as session:
                session.headers.update(headers)

                try:
                    r = session.post(temp_zip_file_url,data=payload, timeout=30)
                    if r.status_code == 200:
                        lis.append(single_date)
                        logger.info(f"File Available for {loop_date}. Downloading...")
                        z = zipfile.ZipFile(io.BytesIO(r.content))
                        df = pd.read_csv(z.open(z.namelist()[0]))
                        filename = z.namelist()[0]
                        df = get_df(df)
                        dfns = pd.concat([df, dfns], ignore_index=True, axis=0)

                        if dfnf.empty:
                            dfnf = copy.deepcopy(df)
                            first_file = filename
                        else:
                            dfnf = pd.merge(df, dfnf, on=['SYMBOL', 'EXPIRY_DT', 'STRIKE_PR', 'OPTION_TYP'], how='left')
                            ext = lis[-2].strftime('%d%b').upper()
                            drop_y(dfnf, ext)
                    else:
                        logger.info(f"File not available for {loop_date} (Status Code: {r.status_code}). Skipping.")
                        skip_dates.append(loop_date)
                        continue

                except requests.exceptions.RequestException as e:
                    logger.error(f"Error downloading the file for {loop_date}: {e}")
                    skip_dates.append(loop_date)
                    continue

    #     dfnf=dfnf.rename(columns={'LOW':'LOW_'+first_file[2:7],'CONTRACTS':'CONTRACTS_'+first_file[2:7],'OPEN':'OPEN_'+first_file[2:7],
    #                              'HIGH':'HIGH_'+first_file[2:7],'CLOSE':'CLOSE_'+first_file[2:7],'OPEN_INT':'OPEN_INT_'+first_file[2:7]})



    if lis[-1].weekday()==5:
        new_date=lis[-1]-timedelta(1)
    elif lis[-1].weekday()==6:
        new_date=lis[-1]-timedelta(2)
    else:
        new_date=lis[-1]

    # loop1_date=new_date.strftime("%Y-%b-%d")
    # year,month,date=loop1_date.split('-')
    # month=month.upper()
    # temp_zip_file_url = 'https://archives.nseindia.com/content/historical/EQUITIES/'+year+'/'+month+'/cm'+date+month+year+'bhav.csv.zip'
    # logger.info(temp_zip_file_url)
    # r = requests.Session().get(temp_zip_file_url)#,verify=False)
    # #r = requests.post(temp_zip_file_url)
    # logger.info("File with status code: "+str(r.status_code))
    # z = zipfile.ZipFile(io.BytesIO(r.content))
    # mtm = pd.read_csv(z.open(z.namelist()[0]))

    lis.sort()

    #print("Number of files downloaded:"+str(No_of_download))
    logger.info("****************************************************************************************") 
    logger.info("No. of files downloaded="+str(No_of_download)) 
    logger.info("Span= " + Start_date+ " to " + End_date )
    logger.info("No. of weekdays in the given time span="+str(Working_day)) 
    logger.info("****************************************************************************************") 
    logging.shutdown()

    return(lis,dfns,dfnf,skip_dates,datetime.now())


# In[ ]:

    
lis,dfns,dfnf,skip_dates,time_dt=downld_data()
df_nf=copy.deepcopy(dfnf)
df_ns=copy.deepcopy(dfns)
# mtm=copy.deepcopy(mtm1)

df_nf.drop(['TIMESTAMP'], axis=1,inplace=True)







# In[63]:



#df_nf,df_ns,lot_size=read_data(filename,Data_names,lot_size)

# mtm=mtm[mtm.SERIES=='EQ']



# df_nf=pd.merge(df_nf,mtm[['SYMBOL','CLOSE']],on="SYMBOL",how="left")
# df_nf.rename(columns={"CLOSE_y":"EQ_price","CLOSE_x":"CLOSE"},inplace=True)

# df_ns=pd.merge(df_ns,mtm[['SYMBOL','CLOSE']],on="SYMBOL",how="left")
# df_ns.rename(columns={"CLOSE_y":"EQ_price","CLOSE_x":"CLOSE"},inplace=True)


if st.sidebar.button("Refresh with Latest Data"):
    st.cache_data.clear()
    st.rerun()
    
with st.sidebar.header('Choose your input type'):
    check_type = st.sidebar.radio('Select your input type here:',('NSE_filter','NSE_stocks'))

time_dt=time_dt.astimezone(pytz.timezone('Asia/Kolkata'))
st.sidebar.write("Last refresh time: ",time_dt.strftime('%d-%b-%Y %I:%M %p'))
next_time=time_dt+timedelta(hours=6)
st.sidebar.write("Next refresh at: ",next_time.strftime('%d-%b-%Y %I:%M %p'))



st.markdown("Data till: "+lis[-1].strftime("%d-%b-%Y"))
if check_type=='NSE_stocks':
    #st.markdown("Data till: "+lis[-1].strftime("%d-%b-%Y"))
    col1,col2,col3,col4,col5=st.columns([2,1.5,1.5,1.5,1.5])
    INSTRUMENTS=col1.radio('Select Stock option or Index option',("Stock Options","Index Options"))
    if INSTRUMENTS=="Stock Options":
        INSTRUMENT='STO'
    elif INSTRUMENTS=="Index Options":
        INSTRUMENT='IDO'
    exp_dates=sorted(pd.to_datetime(df_ns[df_ns.INSTRUMENT==INSTRUMENT].EXPIRY_DT.unique()))
    exp_date=exp_dates[0]
    
    expiry=col5.date_input("Enter expiry date",exp_date)
    expiry=expiry.strftime("%Y-%m-%d")

    df_ns=df_ns[df_ns.INSTRUMENT==INSTRUMENT]
    if expiry not in df_ns.EXPIRY_DT.values:
        st.write('Please select correct expiry among: ')
        lst_exp=[i.strftime("%Y-%m-%d") for i in exp_dates if i.month==datetime.today().month ]
        s = ''
        for i in lst_exp:
            s += "- " + i + "\n"
        st.markdown(s)
        sys.exit()
    
    df_ns=df_ns[df_ns.EXPIRY_DT==expiry]

    l=list(df_ns.SYMBOL)
    if INSTRUMENT=="IDO":
        option = col3.selectbox('Select an index',['BANKNIFTY', 'FINNIFTY', 'MIDCPNIFTY', 'NIFTY'])
    else:        
        option = col3.selectbox('Select a stock',['AARTIIND', 'ABB', 'ABBOTINDIA', 'ABCAPITAL', 'ABFRL', 'ACC', 'ADANIENT', 'ADANIPORTS', 'ALKEM', 'AMARAJABAT', 'AMBUJACEM', 'APOLLOHOSP', 'APOLLOTYRE', 'ASHOKLEY', 'ASIANPAINT', 'ASTRAL', 'ATUL', 'AUBANK', 'AUROPHARMA', 'AXISBANK', 'BAJAJ-AUTO', 'BAJAJFINSV', 'BAJFINANCE', 'BALKRISIND', 'BALRAMCHIN', 'BANDHANBNK', 'BANKBARODA', 'BATAINDIA', 'BEL', 'BERGEPAINT', 'BHARATFORG', 'BHARTIARTL', 'BHEL', 'BIOCON', 'BOSCHLTD', 'BPCL', 'BRITANNIA', 'BSOFT', 'CANBK', 'CANFINHOME', 'CHAMBLFERT', 'CHOLAFIN', 'CIPLA', 'COALINDIA', 'COFORGE', 'COLPAL', 'CONCOR', 'COROMANDEL', 'CROMPTON', 'CUB', 'CUMMINSIND', 'DABUR', 'DALBHARAT', 'DEEPAKNTR', 'DELTACORP', 'DIVISLAB', 'DIXON', 'DLF', 'DRREDDY', 'EICHERMOT', 'ESCORTS', 'EXIDEIND', 'FEDERALBNK', 'FSL', 'GAIL', 'GLENMARK', 'GMRINFRA', 'GNFC', 'GODREJCP', 'GODREJPROP', 'GRANULES', 'GRASIM', 'GSPL', 'GUJGASLTD', 'HAL', 'HAVELLS', 'HCLTECH', 'HDFC', 'HDFCAMC', 'HDFCBANK', 'HDFCLIFE', 'HEROMOTOCO', 'HINDALCO', 'HINDCOPPER', 'HINDPETRO', 'HINDUNILVR', 'HONAUT', 'IBULHSGFIN', 'ICICIBANK', 'ICICIGI', 'ICICIPRULI', 'IDEA', 'IDFC', 'IDFCFIRSTB', 'IEX', 'IGL', 'INDHOTEL', 'INDIACEM', 'INDIAMART', 'INDIGO', 'INDUSINDBK', 'INDUSTOWER', 'INFY', 'INTELLECT', 'IOC', 'IPCALAB', 'IRCTC', 'ITC', 'JINDALSTEL', 'JKCEMENT', 'JSWSTEEL', 'JUBLFOOD', 'KOTAKBANK', 'L&TFH', 'LALPATHLAB', 'LAURUSLABS', 'LICHSGFIN', 'LT', 'LTI', 'LTTS', 'LUPIN', 'M&M', 'M&MFIN', 'MANAPPURAM', 'MARICO', 'MARUTI', 'MCDOWELL-N', 'MCX', 'METROPOLIS', 'MFSL', 'MGL', 'MINDTREE', 'MOTHERSON', 'MPHASIS', 'MRF', 'MUTHOOTFIN', 'NATIONALUM', 'NAUKRI', 'NAVINFLUOR', 'NESTLEIND', 'NMDC', 'NTPC', 'OBEROIRLTY', 'OFSS', 'ONGC', 'PAGEIND', 'PEL', 'PERSISTENT', 'PETRONET', 'PFC', 'PIDILITIND', 'PIIND', 'PNB', 'POLYCAB', 'POWERGRID', 'PVR', 'RAIN', 'RAMCOCEM', 'RBLBANK', 'RECLTD', 'RELIANCE', 'SAIL', 'SBICARD', 'SBILIFE', 'SBIN', 'SHREECEM', 'SIEMENS', 'SRF', 'SRTRANSFIN', 'SUNPHARMA', 'SUNTV', 'SYNGENE', 'TATACHEM', 'TATACOMM', 'TATACONSUM', 'TATAMOTORS', 'TATAPOWER', 'TATASTEEL', 'TCS', 'TECHM', 'TITAN', 'TORNTPHARM', 'TORNTPOWER', 'TRENT', 'TVSMOTOR', 'UBL', 'ULTRACEMCO', 'UPL', 'VEDL', 'VOLTAS', 'WHIRLPOOL', 'WIPRO', 'ZEEL', 'ZYDUSLIFE'])
    s=list(df_ns[df_ns.SYMBOL==option].STRIKE_PR.unique())
    strike_price=col4.selectbox('Select strike price',s)
    #start=st.date_input("Enter start date")
    #end=st.date_input("Enter end date")
    option_type=col2.radio("Option Type",('CE', 'PE'))



    if option and strike_price and option_type and expiry:
        df1=df_ns[(df_ns.SYMBOL==option)&(df_ns.STRIKE_PR==strike_price)&(df_ns.OPTION_TYP==option_type)&(df_ns.EXPIRY_DT==expiry)]
        df1.TIMESTAMP=pd.to_datetime(df1.TIMESTAMP)
        df1=df1.sort_values("TIMESTAMP",ascending=False).reset_index(drop=True)
        df1.drop("INSTRUMENT", axis=1, inplace=True)
        dfx=df1.style.highlight_max(axis=0, props='background-color:lightgreen', subset=['HIGH']).highlight_min(axis=0, color="pink",subset=['LOW']).format(precision=2)

        st.dataframe(dfx)
        s = ''
        st.write("Following dates are skipped (Might be holiday or error), Please check: ")
        for i in skip_dates:
            s += "- " + i + "\n"

        st.markdown(s)
        

    else:
        st.subheader('Please enter all inputs')




elif check_type=='NSE_filter':
    #st.session_state.co=co
    #st.markdown("Data till: "+lis[-1].strftime("%d-%b-%Y"))
    col1,col2,col3,col4=st.columns([2,2,2,2])


    INSTRUMENTS=col1.radio('Select Stock option or Index option',("Stock Options","Index Options"))
    if INSTRUMENTS=="Stock Options":
        INSTRUMENT='STO'
    elif INSTRUMENTS=="Index Options":
        INSTRUMENT='IDO'
    exp_dates=sorted(pd.to_datetime(df_nf[df_nf.INSTRUMENT==INSTRUMENT].EXPIRY_DT.unique()))
    exp_date=exp_dates[0]
    

    # co=int(col4.radio('1-Day or 2-Days decreasing Contracts',(2,1),key='radio_option'))
    co=int(col4.selectbox('1-5 Days decreasing Contracts',(5,4,3,2,1),index=3))
    #st.write(st.session_state.radio_option)



    min_inv=int(col2.text_input('Enter minimum Investments',1000))
    max_inv=int(col3.text_input('Enter maximum Investments',10000))

    col1,buff,col2,col3=st.columns([2,2,2,2])
    close_price=col1.text_input('Minumum price',4)
    contr=col2.text_input('Minumum contracts',200)
    op_int=buff.text_input('Minimum OPEN INTEREST',100000)
    expiry=col3.date_input("Enter expiry date",exp_date)
    expiry=expiry.strftime("%Y-%m-%d")
    print(expiry)


    df_nf=df_nf[df_nf.INSTRUMENT==INSTRUMENT]
    print(df_nf.EXPIRY_DT.unique())
    if expiry not in df_nf.EXPIRY_DT.values:
        st.write('Please select correct expiry among: ')
        lst_exp=[i.strftime("%Y-%m-%d") for i in exp_dates if i.month==datetime.today().month ]
        s = ''
        for i in lst_exp:
            s += "- " + i + "\n"
        st.markdown(s)
        sys.exit()
    
    df_nf=df_nf[df_nf.EXPIRY_DT==expiry]



    lows=df_nf.columns[df_nf.columns.str.contains('LOW')]
    contracts=df_nf.columns[df_nf.columns.str.contains('CONTRACTS')]
    df_nf=df_nf[df_nf.LOW==df_nf[lows].min(axis=1)]

    if INSTRUMENT=='STO':
        df_ce=df_nf[(df_nf.OPTION_TYP=='CE')]#&(df_nf.STRIKE_PR>df_nf.EQ_price)]
        df_pe=df_nf[(df_nf.OPTION_TYP=='PE')]#&(df_nf.STRIKE_PR<df_nf.EQ_price)]
        df2=pd.concat([df_ce, df_pe], ignore_index=True, axis=0)
    else:
        df2=df_nf



    #print(yest_con_name)
    #Add button **************************************
    df4=df2[df2.loc[:,contracts].apply(lambda x: all(x[i]<x[i+1] for i in range(co)),axis=1)]



#     df4=df2.merge(lot_size,on='SYMBOL',how="left")
#     df4.rename(columns={'JAN-23':'Lot_size'},inplace=True)
    df4.Lot_size=df4.Lot_size.astype('int64')
    df4['Investment']=df4['HIGH']*df4['Lot_size']



    if (close_price) and (not contr) and (not op_int):
        close_price=int(close_price)
        df10=df4[(df4.Investment>min_inv)&(df4.Investment<=max_inv)&(df4.CLOSE>close_price)].reset_index(drop=True)
    elif (contr) and (not close_price) and (not op_int):
        contr=int(contr)
        df10=df4[(df4.Investment>min_inv)&(df4.Investment<=max_inv)&(df4.CONTRACTS>contr)].reset_index(drop=True)
    elif (op_int) and (not close_price) and (not contr):
        op_int=int(op_int)
        df10=df4[(df4.Investment>min_inv)&(df4.Investment<=max_inv)&(df4['OPEN_INT']>op_int)].reset_index(drop=True)
    elif (op_int) and (close_price) and (contr):
        close_price=int(close_price)
        contr=int(contr)
        op_int=int(op_int)
        df10=df4[(df4.Investment>min_inv)&(df4.Investment<=max_inv)&(df4['OPEN_INT']>op_int)&(df4.CLOSE>close_price)&(df4.CONTRACTS>contr)].reset_index(drop=True)
    else:
        df10=df4[(df4.Investment>min_inv)&(df4.Investment<=max_inv)].reset_index(drop=True)

    # df_ce1=df10[df10.OPTION_TYP=='CE'].drop_duplicates(subset=['SYMBOL','OPTION_TYP'],keep='first',ignore_index=True)
    # df_pe1=df10[df10.OPTION_TYP=='PE'].drop_duplicates(subset=['SYMBOL','OPTION_TYP'],keep='last',ignore_index=True)
    # df11=pd.concat([df_ce1, df_pe1], ignore_index=True, axis=0)




    #style.highlight_max(axis=0)
    df11=df10[['SYMBOL', 'EXPIRY_DT', 'STRIKE_PR', 'OPTION_TYP',
           'OPEN', 'HIGH', 'LOW', 'CLOSE', 'OPEN_INT','CONTRACTS','EQ_price', 'Lot_size', 'Investment']]

    st.dataframe(df11.style.format(precision=2))
    if skip_dates:
        s = ''
        st.write("Following dates are skipped (Might be holiday or error), Please check: ")
        for i in skip_dates:
            s += "- " + i + "\n"
    
        st.markdown(s)

    reports_csv=df11.to_csv().encode('utf-8')
    st.download_button(label="Export Report",data=reports_csv,file_name='Report.csv',mime='text/csv')
# else:
# st.subheader("Please click on 'Download Data'")
