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
import os
from datetime import datetime,date,timedelta

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
    df = df.applymap(lambda x: x.strip() if type(x)==str else x)
#     df = df[df.INSTRUMENT==INSTRUMENT]
#     df = df[df.EXPIRY_DT==expiry]
    df=df[['TIMESTAMP','INSTRUMENT','SYMBOL', 'EXPIRY_DT', 'STRIKE_PR', 'OPTION_TYP',
           'OPEN', 'HIGH', 'LOW', 'CLOSE', 'OPEN_INT', 'CONTRACTS']]
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



#@st.cache
def read_data(df,df_ns,df_nf,filename):
    df=get_df(df)
    df_nf=pd.merge(df,df_nf,on=['SYMBOL', 'EXPIRY_DT', 'STRIKE_PR', 'OPTION_TYP'],how='left')
    drop_y(df_nf,filename)
    df_ns=pd.concat([df,df_ns],ignore_index=True,axis=0)
    return df_nf,df_ns
    #print(i)
    
    



def req(zip_file_url,df_ns,df_nf):
    global No_of_download
    r = requests.post(zip_file_url)
    status_code=r.status_code
        
    #print(status_code)
    #If status code is <> 200, it indicates that no data is present for that date. For example, week-end, or trading holiday.
    if status_code==200:
        No_of_download=No_of_download+1
        logger.info("File Available.Downloading")
        z = zipfile.ZipFile(io.BytesIO(r.content))
        #z.extractall(path=path)
        df = pd.read_csv(z.open(z.namelist()[0]))
        print(df.columns)
        if (df_nf.empty) & (df_ns.empty):
            df_nf=get_df(df)
            df_ns=get_df(df)
        else:
            df_nf,df_ns=read_data(df,df_ns,df_nf,z.namelist()[0])
    else:
        logger.info("******File Not Available.Moving to next date.")
    return status_code,df_nf,df_ns
    





today_date=datetime.now().strftime("%Y%b%d")
logging.basicConfig(filename="Log_"+today_date+".log", format='%(asctime)s %(message)s', filemode='w') 
logger=logging.getLogger() 
logger.setLevel(logging.INFO) 



#Populating today's date as default, if the stat_date and/or End_date is not provided.
@st.cache(ttl=86400)
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
        Start_date=(datetime.now()-timedelta(days=14)).strftime("%Y%b%d")
        End_date=today_date
    if End_date=="" or End_date=="enter_start_date_in_DDMMMYYYY":
            End_date=today_date

    daterange = pd.date_range(datetime.strptime(Start_date, "%Y%b%d"),datetime.strptime(End_date, "%Y%b%d"))
    lis=[]
    #Looping through each date, and downloading the file.
    for single_date in daterange:
        loop_date=single_date.strftime("%Y-%b-%d")
        year,month,date=loop_date.split('-')
        month=month.upper()
        weekday=single_date.weekday()
        #If day is not Saturday or Sunday,then proceed to download the file.
        if weekday not in [5,6]:
            Working_day=Working_day+1
            logger.info("Trying to download File of :"+loop_date)
            temp_zip_file_url = 'https://www1.nseindia.com/content/historical/DERIVATIVES/'+year+'/'+month+'/fo'+date+month+year+'bhav.csv.zip'
            #print(temp_zip_file_url)
            #ls,df_ns,df_nf=req(temp_zip_file_url,df_ns,df_nf)
            r = requests.post(temp_zip_file_url)
            status_code=r.status_code
            if status_code==200:
                lis.append(single_date)
                logger.info("File Available.Downloading")
                z = zipfile.ZipFile(io.BytesIO(r.content))
                df = pd.read_csv(z.open(z.namelist()[0]))
                filename=z.namelist()[0]
                df=get_df(df)
                dfns=pd.concat([df,dfns],ignore_index=True,axis=0)
                if dfnf.empty:
                    dfnf=copy.deepcopy(df)
                    first_file=filename

                else:

                    dfnf=pd.merge(df,dfnf,on=['SYMBOL', 'EXPIRY_DT', 'STRIKE_PR', 'OPTION_TYP'],how='left')
                    ext=lis[-2].strftime('%d%b').upper()
                    drop_y(dfnf,ext)

#     dfnf=dfnf.rename(columns={'LOW':'LOW_'+first_file[2:7],'CONTRACTS':'CONTRACTS_'+first_file[2:7],'OPEN':'OPEN_'+first_file[2:7],
#                              'HIGH':'HIGH_'+first_file[2:7],'CLOSE':'CLOSE_'+first_file[2:7],'OPEN_INT':'OPEN_INT_'+first_file[2:7]})



    if lis[-1].weekday()==5:
        new_date=lis[-1]-timedelta(1)
    elif lis[-1].weekday()==6:
        new_date=lis[-1]-timedelta(2)
    else:
        new_date=lis[-1]
    loop1_date=new_date.strftime("%Y-%b-%d")
    year,month,date=loop1_date.split('-')
    month=month.upper()
    temp_zip_file_url = 'https://www1.nseindia.com/content/historical/EQUITIES/'+year+'/'+month+'/cm'+date+month+year+'bhav.csv.zip'
    logger.info(temp_zip_file_url)
    r = requests.post(temp_zip_file_url)
    logger.info("File with status code: "+str(r.status_code))
    z = zipfile.ZipFile(io.BytesIO(r.content))
    mtm = pd.read_csv(z.open(z.namelist()[0]))
    
    lis.sort()
    
    #print("Number of files downloaded:"+str(No_of_download))
    logger.info("****************************************************************************************") 
    logger.info("No. of files downloaded="+str(No_of_download)) 
    logger.info("Span= " + Start_date+ " to " + End_date )
    logger.info("No. of weekdays in the given time span="+str(Working_day)) 
    logger.info("****************************************************************************************") 
    logging.shutdown()
    
    
    lot_size=pd.read_csv('fo_mktlots.csv')

    return(lis,dfns,dfnf,lot_size,mtm)


# In[ ]:

    
lis,dfns,dfnf,lot_size,mtm1=downld_data()
df_nf=copy.deepcopy(dfnf)
df_ns=copy.deepcopy(dfns)
mtm=copy.deepcopy(mtm1)

df_nf.drop(['TIMESTAMP'], axis=1,inplace=True)
lot_size.columns=lot_size.columns.str.strip()
lot_size=lot_size[['SYMBOL','JAN-23']]
lot_size = lot_size.applymap(lambda x: x.strip() if type(x)==str else x)
for i in lot_size['JAN-23']:
    try:
        int(i)
    except ValueError:
        #print(i)
        lot_size.drop(lot_size[lot_size['JAN-23']==i].index, inplace = True)
lot_size['JAN-23']=lot_size['JAN-23'].astype(int)






# In[63]:



#df_nf,df_ns,lot_size=read_data(filename,Data_names,lot_size)

mtm=mtm[mtm.SERIES=='EQ']



df_nf=pd.merge(df_nf,mtm[['SYMBOL','CLOSE']],on="SYMBOL",how="left")
df_nf.rename(columns={"CLOSE_y":"EQ_price","CLOSE_x":"CLOSE"},inplace=True)

df_ns=pd.merge(df_ns,mtm[['SYMBOL','CLOSE']],on="SYMBOL",how="left")
df_ns.rename(columns={"CLOSE_y":"EQ_price","CLOSE_x":"CLOSE"},inplace=True)




with st.sidebar.header('Choose your input type'):
    check_type = st.sidebar.selectbox('Select your input type here:',('NSE_filter','NSE_stocks'))

st.sidebar.write('Your selected input type:', check_type)



st.markdown("Data till: "+lis[-1].strftime("%d-%b-%Y"))
if check_type=='NSE_stocks':
    #st.markdown("Data till: "+lis[-1].strftime("%d-%b-%Y"))
    col1,col2,col3,col4,col5=st.columns([2,1.5,1.5,1.5,1.5])
    INSTRUMENT=col1.radio('Select Stock option or Index option',("OPTSTK","OPTIDX"))
    
    expiry=col5.date_input("Enter expiry date",nthu)
    expiry=expiry.strftime("%d-%b-%Y")

    df_ns=df_ns[df_ns.INSTRUMENT==INSTRUMENT]
    df_ns=df_ns[df_ns.EXPIRY_DT==expiry]

    l=list(df_ns.SYMBOL)
    if INSTRUMENT=="OPTIDX":
        option = col3.selectbox('Please select an index',['BANKNIFTY', 'FINNIFTY', 'MIDCPNIFTY', 'NIFTY'])
    else:        
        option = col3.selectbox('Please select a stock',['AARTIIND', 'ABB', 'ABBOTINDIA', 'ABCAPITAL', 'ABFRL', 'ACC', 'ADANIENT', 'ADANIPORTS', 'ALKEM', 'AMARAJABAT', 'AMBUJACEM', 'APOLLOHOSP', 'APOLLOTYRE', 'ASHOKLEY', 'ASIANPAINT', 'ASTRAL', 'ATUL', 'AUBANK', 'AUROPHARMA', 'AXISBANK', 'BAJAJ-AUTO', 'BAJAJFINSV', 'BAJFINANCE', 'BALKRISIND', 'BALRAMCHIN', 'BANDHANBNK', 'BANKBARODA', 'BATAINDIA', 'BEL', 'BERGEPAINT', 'BHARATFORG', 'BHARTIARTL', 'BHEL', 'BIOCON', 'BOSCHLTD', 'BPCL', 'BRITANNIA', 'BSOFT', 'CANBK', 'CANFINHOME', 'CHAMBLFERT', 'CHOLAFIN', 'CIPLA', 'COALINDIA', 'COFORGE', 'COLPAL', 'CONCOR', 'COROMANDEL', 'CROMPTON', 'CUB', 'CUMMINSIND', 'DABUR', 'DALBHARAT', 'DEEPAKNTR', 'DELTACORP', 'DIVISLAB', 'DIXON', 'DLF', 'DRREDDY', 'EICHERMOT', 'ESCORTS', 'EXIDEIND', 'FEDERALBNK', 'FSL', 'GAIL', 'GLENMARK', 'GMRINFRA', 'GNFC', 'GODREJCP', 'GODREJPROP', 'GRANULES', 'GRASIM', 'GSPL', 'GUJGASLTD', 'HAL', 'HAVELLS', 'HCLTECH', 'HDFC', 'HDFCAMC', 'HDFCBANK', 'HDFCLIFE', 'HEROMOTOCO', 'HINDALCO', 'HINDCOPPER', 'HINDPETRO', 'HINDUNILVR', 'HONAUT', 'IBULHSGFIN', 'ICICIBANK', 'ICICIGI', 'ICICIPRULI', 'IDEA', 'IDFC', 'IDFCFIRSTB', 'IEX', 'IGL', 'INDHOTEL', 'INDIACEM', 'INDIAMART', 'INDIGO', 'INDUSINDBK', 'INDUSTOWER', 'INFY', 'INTELLECT', 'IOC', 'IPCALAB', 'IRCTC', 'ITC', 'JINDALSTEL', 'JKCEMENT', 'JSWSTEEL', 'JUBLFOOD', 'KOTAKBANK', 'L&TFH', 'LALPATHLAB', 'LAURUSLABS', 'LICHSGFIN', 'LT', 'LTI', 'LTTS', 'LUPIN', 'M&M', 'M&MFIN', 'MANAPPURAM', 'MARICO', 'MARUTI', 'MCDOWELL-N', 'MCX', 'METROPOLIS', 'MFSL', 'MGL', 'MINDTREE', 'MOTHERSON', 'MPHASIS', 'MRF', 'MUTHOOTFIN', 'NATIONALUM', 'NAUKRI', 'NAVINFLUOR', 'NESTLEIND', 'NMDC', 'NTPC', 'OBEROIRLTY', 'OFSS', 'ONGC', 'PAGEIND', 'PEL', 'PERSISTENT', 'PETRONET', 'PFC', 'PIDILITIND', 'PIIND', 'PNB', 'POLYCAB', 'POWERGRID', 'PVR', 'RAIN', 'RAMCOCEM', 'RBLBANK', 'RECLTD', 'RELIANCE', 'SAIL', 'SBICARD', 'SBILIFE', 'SBIN', 'SHREECEM', 'SIEMENS', 'SRF', 'SRTRANSFIN', 'SUNPHARMA', 'SUNTV', 'SYNGENE', 'TATACHEM', 'TATACOMM', 'TATACONSUM', 'TATAMOTORS', 'TATAPOWER', 'TATASTEEL', 'TCS', 'TECHM', 'TITAN', 'TORNTPHARM', 'TORNTPOWER', 'TRENT', 'TVSMOTOR', 'UBL', 'ULTRACEMCO', 'UPL', 'VEDL', 'VOLTAS', 'WHIRLPOOL', 'WIPRO', 'ZEEL', 'ZYDUSLIFE'])
    s=list(df_ns[df_ns.SYMBOL==option].STRIKE_PR.unique())
    strike_price=col4.selectbox('Please select strike price',s)
    #start=st.date_input("Enter start date")
    #end=st.date_input("Enter end date")
    option_type=col2.radio("Option Type",('CE', 'PE'))



    if option and strike_price and option_type and expiry:
        df1=df_ns[(df_ns.SYMBOL==option)&(df_ns.STRIKE_PR==strike_price)&(df_ns.OPTION_TYP==option_type)&(df_ns.EXPIRY_DT==expiry)]
        df1.TIMESTAMP=pd.to_datetime(df1.TIMESTAMP)
        df1=df1.sort_values("TIMESTAMP",ascending=False).reset_index(drop=True)
        df1.drop("INSTRUMENT", axis=1, inplace=True)
        dfx=df1.style.highlight_max(axis=0, props='background-color:lightgreen', subset=['HIGH']).highlight_min(axis=0, color="pink",subset=['LOW']).set_precision(2)

        st.dataframe(dfx)


    else:
        st.subheader('Please enter all inputs')




elif check_type=='NSE_filter':
    #st.session_state.co=co
    #st.markdown("Data till: "+lis[-1].strftime("%d-%b-%Y"))
    col1,col2,col3,col4=st.columns([2,2,2,2])


    INSTRUMENT=col1.radio('Select Stock option or Index option',("OPTSTK","OPTIDX"))

    co=int(col4.radio('1-Day or 2-Days decreasing Contracts',(2,1),key='radio_option'))
    #st.write(st.session_state.radio_option)



    min_inv=int(col2.radio('Enter minimum Investments',(1000,3000,5000,10000)))
    max_inv=int(col3.radio('Enter maximum Investments',(10000,5000,3000,)))

    col1,buff,col2,col3=st.columns([2,2,2,2])
    close_price=col1.text_input('Minumum price',4)
    contr=col2.text_input('Minumum contracts',200)
    op_int=buff.text_input('Minimum OPEN INTEREST',100000)
    expiry=col3.date_input("Enter expiry date",nthu)
    expiry=expiry.strftime("%d-%b-%Y")


    df_nf=df_nf[df_nf.INSTRUMENT==INSTRUMENT]
    df_nf=df_nf[df_nf.EXPIRY_DT==expiry]




    today_con_name="CONTRACTS"
    yest_con_name="CONTRACTS_"+lis[-3:][1].strftime('%d%b').upper()
    daybef_con_name="CONTRACTS_"+lis[-3:][0].strftime('%d%b').upper()



    #df_nf=df_nf.rename(columns={today_con_name:"CONTRACTS",'LOW_'+exten:"LOW"})
#     df_nf=df_nf.rename(columns={today_con_name:"CONTRACTS",'LOW_'+exten:"LOW",'OPEN_'+exten:'OPEN',
#                                  'HIGH_'+exten:'HIGH','CLOSE_'+exten:'CLOSE','OPEN_INT_'+exten:'OPEN_INT'})



    lows=df_nf.columns[df_nf.columns.str.contains('LOW')]
    contracts=df_nf.columns[df_nf.columns.str.contains('CONTRACTS')]
    OI=df_nf.columns[df_nf.columns.str.contains('OPEN_INT')]

    df_nf=df_nf[df_nf.LOW==df_nf[lows].min(axis=1)]

    if INSTRUMENT=='OPTSTK':
        df_ce=df_nf[(df_nf.OPTION_TYP=='CE')&(df_nf.STRIKE_PR>df_nf.EQ_price)]
        df_pe=df_nf[(df_nf.OPTION_TYP=='PE')&(df_nf.STRIKE_PR<df_nf.EQ_price)]
        df2=pd.concat([df_ce, df_pe], ignore_index=True, axis=0)
    else:
        df2=df_nf



    #print(yest_con_name)
    #Add butooon **************************************
    if co==1:
        df4=df2[(df2["CONTRACTS"]<df2[yest_con_name])]
    else:
        df4=df2[(df2["CONTRACTS"]<df2[yest_con_name])&(df2[yest_con_name]<df2[daybef_con_name])]


    today_OI_name="OPEN_INT"
    yest_OI_name="OPEN_INT_"+lis[-3:][1].strftime('%d%b').upper()



    df4=df4.merge(lot_size,on='SYMBOL',how="left")
    df4.rename(columns={'JAN-23':'Lot_size'},inplace=True)
    #df4.Lot_size=df4.Lot_size.astype('int64')
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

    df_ce1=df10[df10.OPTION_TYP=='CE'].drop_duplicates(subset=['SYMBOL','OPTION_TYP'],keep='first',ignore_index=True)
    df_pe1=df10[df10.OPTION_TYP=='PE'].drop_duplicates(subset=['SYMBOL','OPTION_TYP'],keep='last',ignore_index=True)
    df11=pd.concat([df_ce1, df_pe1], ignore_index=True, axis=0)




    #style.highlight_max(axis=0)
    df11=df11[['SYMBOL', 'EXPIRY_DT', 'STRIKE_PR', 'OPTION_TYP',
           'OPEN', 'HIGH', 'LOW', 'CLOSE', 'OPEN_INT','CONTRACTS','EQ_price', 'Lot_size', 'Investment']]

    st.dataframe(df11.style.set_precision(2))

    reports_csv=df11.to_csv().encode('utf-8')
    st.download_button(label="Export Report",data=reports_csv,file_name='Report.csv',mime='text/csv')
# else:
# st.subheader("Please click on 'Download Data'")
