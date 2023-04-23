import pandas as pd
import streamlit as st
import requests, zipfile, io,logging
import shutil
import copy
import os
from datetime import datetime,date,timedelta

from dateutil.relativedelta import relativedelta, TH

st.set_page_config(layout="wide")


def get_df(df):
    #df = pd.read_csv(d_path+'/'+name)
    df.columns = df.columns.str.strip()
    df = df.applymap(lambda x: x.strip() if type(x)==str else x)
    df = df[df.SERIES=="EQ"]
#     df = df[df.EXPIRY_DT==expiry]
    df=df[['TIMESTAMP','SYMBOL', 'OPEN', 'HIGH', 'LOW', 'CLOSE', 'TOTTRDQTY']]
    df.rename(columns={"TOTTRDQTY":"VOLUME"}, inplace=True)
    df.reset_index(drop=True,inplace=True)

    return df





def drop_y(df,filename):
    # list comprehension of the cols that end with '_y'
    to_drop = [x for x in df if x.endswith('_y')]
    if "LOW_y" in to_drop:
        to_drop.remove("LOW_y")
    if to_drop:
        df.drop(to_drop, axis=1, inplace=True)
        rename_x(df,filename)
def rename_x(df,filename):
    for col in df:
        if col.endswith('_x'):
            df.rename(columns={col:col.rstrip('_x')}, inplace=True)
        elif col.endswith('_y'):
            df.rename(columns={col:col.rstrip('_y')+'_'+filename},inplace=True)




            
            
today_date=datetime.now().strftime("%Y%b%d")
logging.basicConfig(filename="Log_"+today_date+".log", format='%(asctime)s %(message)s', filemode='w') 
logger=logging.getLogger() 
logger.setLevel(logging.INFO) 



#Populating today's date as default, if the stat_date and/or End_date is not provided.
@st.cache(ttl=21600)
def downld_data():
    
    dfns=pd.DataFrame()
    dfnf=pd.DataFrame()
    global No_of_download,Working_day,Non_Work_day
    #df_ns,df_nf=pd.DataFrame(),pd.DataFrame()
    No_of_download=0
    Working_day=0
    Non_Work_day=0
    Start_date=(datetime.now()+timedelta(days=1))
    check=True
    count,count2=0,0

    
    lis=[]
    #Looping through each date, and downloading the file.
    single_date=Start_date
    while count<90:
        
        ### for infite loop
        count2+=1
        if count2>200:
            break;
         
        single_date=single_date-timedelta(days=1)
        loop_date=single_date.strftime("%Y-%b-%d")
        year,month,date=loop_date.split('-')
        month=month.upper()
        weekday=single_date.weekday()
        #If day is not Saturday or Sunday,then proceed to download the file.
        if weekday not in [5,6]:
            Working_day=Working_day+1
            logger.info("Trying to download File of :"+loop_date)
            temp_zip_file_url = 'https://archives.nseindia.com/content/historical/EQUITIES/'+year+'/'+month+'/cm'+date+month+year+'bhav.csv.zip'
            
            try:
                r = requests.Session().get(temp_zip_file_url,timeout=5)
            except:                
                logger.info("File not Available. Skipping....")
                continue;
            
            status_code=r.status_code
            if status_code==200:
                count+=1
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

                    dfnf=pd.merge(dfnf,df,on=['SYMBOL'],how='left')
                    ext=lis[-2].strftime('%d%b').upper()
                    drop_y(dfnf,ext)
            else:
                logger.info("******File Not Available.Moving to next date.")
    
    
    lis.sort()
    
    #print("Number of files downloaded:"+str(No_of_download))
    logger.info("****************************************************************************************") 
    logger.info("No. of files downloaded="+str(count)) 
    logger.info("Span= " + Start_date.strftime("%Y-%b-%d")+ " to " + loop_date )
    logger.info("No. of weekdays in the given time span="+str(Working_day)) 
    logger.info("****************************************************************************************") 
    logging.shutdown()
    #print(first_file)

    return(lis,dfns,dfnf)




lis,dfns,dfnf=downld_data()
df_nf=copy.deepcopy(dfnf)
df_ns=copy.deepcopy(dfns)


#df_nf.drop(['TIMESTAMP'], axis=1,inplace=True)




with st.sidebar.header('Choose your input type'):
    check_type = st.sidebar.selectbox('Select your input type here:',('NSE_filter','NSE_stocks'))

st.sidebar.write('Your selected input type:', check_type)


st.markdown('Latest Data is of : '+lis[-1].strftime("%d-%b-%Y"))
if check_type=='NSE_stocks':
    
    INSTRUMENT=st.selectbox('Select a Stock',df_ns.SYMBOL.unique())
    



    if INSTRUMENT:
        df1=df_ns[(df_ns.SYMBOL==INSTRUMENT)]
        df1.TIMESTAMP=pd.to_datetime(df1.TIMESTAMP)
        df1=df1.sort_values("TIMESTAMP",ascending=False).reset_index(drop=True)
        #df1.drop("INSTRUMENT", axis=1, inplace=True)
        df1['TIMESTAMP'] = pd.to_datetime(df1['TIMESTAMP']).dt.date
        dfx=df1.style.highlight_max(axis=0, props='background-color:lightgreen', subset=['HIGH']).highlight_min(axis=0, color="pink",subset=['LOW']).set_precision(2)
        
        if df1.empty:
            st.subheader('No data')
        else:
            st.dataframe(dfx)
         

    else:
        st.subheader('Please enter all inputs')
        
        
        
        
elif check_type=='NSE_filter':
    
    col2,col3=st.columns([2,2])

    min_inv=int(col2.text_input('Enter minimum Investments',100))
    max_inv=int(col3.text_input('Enter maximum Investments',5000))
    
   
    lows=df_nf.columns[df_nf.columns.str.contains('LOW')]
    print(lows)
    
    df_nf=df_nf[df_nf.LOW==df_nf[lows].min(axis=1)]
    df4=df_nf

    df10=df4[(df4.CLOSE>min_inv)&(df4.CLOSE<=max_inv)].reset_index(drop=True)
    
    

    #style.highlight_max(axis=0)
    df11=df10[['TIMESTAMP','SYMBOL','OPEN', 'HIGH', 'LOW', 'CLOSE','VOLUME']]

    st.dataframe(df11.style.set_precision(2))

    reports_csv=df11.to_csv().encode('utf-8')
    st.download_button(label="Export Report",data=reports_csv,file_name='Report.csv',mime='text/csv')
    # else:
    #     st.subheader("Please click on 'Download Data'")
