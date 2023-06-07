import pandas as pd
import calendar
import numpy as np
from datetime import datetime as dt
import time

import tempfile
import shutil
import os
import pandas as pd
from google.cloud import storage

from sqlalchemy import create_engine
import psycopg2
import postgresql_credentials #Here I created a folder with the username and password for the database access


class ipc_updater():
    
    def __init__(self):
        
        
        self.data=self.get_data()
        
        self.max_date_db= self.querySQL('''select max(period_date) as period_date from reba_ipc''').loc[0,'period_date']
        
        self.run()
         
        
    def timing_decorator(func):
        def wrapper(*args, **kwargs):
            start_time = time.time()
            result = func(*args, **kwargs)
            end_time = time.time()
            print(f"Function {func.__name__} took {end_time - start_time} seconds to run.")
            return result
        return wrapper
    
    @timing_decorator
    def get_data(self):
        dump=pd.read_excel("https://www.indec.gob.ar/ftp/cuadros/economia/sh_ipc_aperturas.xls",sheet_name='Índices aperturas')
        dump.columns=np.array(range(len(dump.columns)))
        dump.replace(np.nan,'blank_space',inplace=True)
        index_to_drop=[]
        for i in range(0,len(dump)):
            if 'blank_space' in dump.iloc[i].value_counts() and dump.iloc[i].value_counts()['blank_space']>=dump.shape[1]-1:
                index_to_drop.append(i)
                
        dump1=dump.drop(index=index_to_drop).reset_index(drop=True)
        
        personal_care=dump1.index[dump1[0] == "Cuidado personal"].tolist()
        
        test_regiones=[region+1 for region in personal_care[:5]]
        test_regiones.insert(0,0)
        
        
        dfs={}
        for z,x in zip(test_regiones,personal_care):
            dfs[dump1.iloc[z,0][7:].lower()]=dump1.iloc[z:x+1].reset_index(drop=True)
            
        table={ 'region':[],
                'period_date':[],
              'category':[],
              'ipc_accumulated':[]}
        
        for k in dfs.keys():
            
            new_columns=dfs[k].iloc[0][1:].tolist()
            new_index=dfs[k][0][1:].tolist()
            dfs[k]=dfs[k].drop(index=0)
            dfs[k].set_index(0,inplace=True)
            dfs[k].columns=new_columns
            dfs[k].iloc[:,[0]]=np.ones((dfs[k].shape[0],1), dtype=int) 
            
            for r in dfs[k].index:
                
                for c in dfs[k].columns:
                    
                    table['region'].append(k)
                    table['period_date'].append(pd.Period(f"{c.year}-{c.month}",freq='M'))
                    table['category'].append(r)
                    table['ipc_accumulated'].append(dfs[k].loc[r,c])
        
        data=pd.DataFrame(table)
        data=data.sort_values(by=['region','category','period_date'],ascending=[True,True,True]).reset_index(drop=True)
        
        data['ipc_accumulated']=data.ipc_accumulated.map(lambda x: (x/100) if x!=1 and isinstance(x,str)==False else np.NaN if isinstance(x,str)==True else x)
        
        
        data['ipc_accumulated'].fillna(method='ffill',inplace=True)
        uncumsum = data.groupby(['region','category']).diff().fillna(0).reset_index(drop=True)
        
        uncumsum=uncumsum.rename(columns={'ipc_accumulated':'inflation'}).loc[:,['inflation']]
        
        data=data.merge(uncumsum,left_index=True,right_index=True).sort_values(by=['period_date','region','category']).reset_index(drop=True)
        
        
        data.period_date=data.period_date.map(lambda x: dt(year=x.year,month=x.month,day=calendar.monthrange(x.year,x.month)[1]))
        
        return data
    
    def querySQL(self,query,db='reba_challenge'):
        
        try:
            conn= psycopg2.connect(user=postgresql_credentials.username,
                                          password=postgresql_credentials.password,
                                          host="localhost",
                                          port="5432",
                                          database=db)
        except (Exception, psycopg2.Error) as error:
            print("Error while fetching data from PostgreSQL", error)
            
        if conn:
            
            try:
                cursor = conn.cursor()
                cursor.execute(query)
                data = cursor.fetchall()
                cols = list(map(lambda x: x[0], cursor.description))
                response = pd.DataFrame(data, columns=cols)
        
            except (Exception, psycopg2.Error) as error:
                print("Error while fetching data from PostgreSQL", error)
                response=''
            
            cursor.close()
            conn.close()
    
        return response
    
    
    
    @timing_decorator
    def sql_uploader(self,data):
               
        try: 
            db = create_engine(f"postgresql://{postgresql_credentials.username}:{postgresql_credentials.password}@localhost:5432/reba_challenge")
            conn = db.connect()
            
            if conn:
                #
                data.to_sql('reba_ipc', con=conn, if_exists='append',chunksize=1000, index=False)
                #print("CARGO DATA DB")
                print(f"{len(data)} rows have been uploaded")
            conn.close()
            return True
        except:
            print("Fail to create engine")        
            return False
    
    @timing_decorator
    def bucket_uploader(self,data):        
            
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"]= "" 
            bucket_name = 'reba_challenge'
            destination_blob_path = f"data_ipc_{data.period_date.max().year}-{data.period_date.max().month}.csv"
            
            def upload_to_storage(bucket_name: str, source_file_path: str, destination_blob_path: str):
                """Uploads a file to the bucket."""
                storage_client = storage.Client()
                bucket = storage_client.get_bucket(bucket_name)
                blob = bucket.blob(destination_blob_path)
                blob.upload_from_filename(source_file_path)
                print(f'The file {source_file_path} is uploaded to GCP bucket path: {destination_blob_path}')
                return None
                 
            def list_blobs(bucket_name=str):
                """Lists all the blobs in the bucket."""
                response=[]
                bucket_name = "reba_challenge"
            
                storage_client = storage.Client()
            
                # Note: Client.list_blobs requires at least package version 1.17.0.
                blobs = storage_client.list_blobs(bucket_name)
            
                # Note: The call returns a response only when the iterator is consumed.
                for blob in blobs:
                    response.append(blob.name)
                return response
            
            
            if destination_blob_path not in list_blobs():
                
                try:
                    dir0 = os.getcwd()
                    ## create temporary directory:
                    vr_dir = tempfile.mkdtemp()
                    ## moves to temporary directory:
                    os.chdir(vr_dir)

                    data.to_csv(f"data_ipc_{data.period_date.max().year}-{data.period_date.max().month}.csv",index=False)

                    source_file_path = f"{vr_dir}\\data_ipc_{data.period_date.max().year}-{data.period_date.max().month}.csv"
                    ## cargo el bucket
                    upload_to_storage(bucket_name, source_file_path, destination_blob_path)
                    #print("CARGO INFO BUCKET")
                    print(f"data_ipc_{data.period_date.max().year}-{data.period_date.max().month}.csv has been uploaded succesfully")
                
                except:
                    print("Failed to upload data to bucket")

                finally:
                    ## leaves temporary directory: (to enable close)
                    os.chdir(dir0)
                    ## close temporaty directory:
                    shutil.rmtree(vr_dir)
            else:
                print(f"{destination_blob_path} already loaded in bucket")
                
    @timing_decorator          
    def run(self):
        
        if self.max_date_db<self.data.period_date.max():
            
            print("There is data to upload")
            
            data_to_upload=self.data[self.data['period_date']>self.max_date_db].reset_index(drop=True)
    
            data_to_sql=self.sql_uploader(data_to_upload)
            
            if data_to_sql:
                self.bucket_uploader(self.data)
            else:
                print("Nothing to upload to gcp")
        else:
            print("Nothing to upload to db")

