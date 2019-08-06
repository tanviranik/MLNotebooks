#Date 30/07/2019
#Author: Md. Sanaul Karim
#Associated Python Script of notebooks RFMAnalysisAndCustomerClusteringOutletWiseAgglomerativeClustering.ipynb
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import datetime
import pyodbc
import sqlalchemy as sa
from sklearn.preprocessing import MinMaxScaler
from sklearn.cluster import AgglomerativeClustering

#Database connection and data fetching from Database to dataframe
try:    
     conn = pyodbc.connect("Driver={SQL Server Native Client 11.0};"
                           "Server=192.168.11.200;"
                           "Database=EPSMirror;uid=sa;pwd=flexiload;"
                           "Trusted_Connection=no;")
     #fetching data from sql server store procedure
     df = pd.read_sql("EXEC dbo.SP_RawDataFromChurn 'Jan 01 2019','Jun 30 2019', 'D007';", conn)
     print("Connection Established")

except Exception as exp:
          print("Can not Connect")




#ResearchId
research_id=df['ResearchId'][0]

#taking first first four columns
transaction_data=df.iloc[:,0:4].copy()
#converting to invoiceDate to date time type
transaction_data['InvoiceDate']=transaction_data['InvoiceDate'].apply(pd.to_datetime)

#Building recency feature
reference_date = transaction_data.InvoiceDate.max()
reference_date = reference_date + datetime.timedelta(days = 1)
transaction_data['days_since_last_purchase'] = reference_date - transaction_data.InvoiceDate
transaction_data['days_since_last_purchase_num'] = transaction_data['days_since_last_purchase'].astype('timedelta64[D]')
customer_history_df = transaction_data.groupby("CustomerCode").min().reset_index()[['CustomerCode', 'days_since_last_purchase_num']]
customer_history_df.rename(columns={'days_since_last_purchase_num':'recency'}, inplace=True)

#building frequency and monetary features
customer_monetary_val = transaction_data[['CustomerCode', 'Amount']].groupby("CustomerCode").sum().reset_index()
customer_history_df = customer_history_df.merge(customer_monetary_val, how='outer')
customer_history_df.Amount = customer_history_df.Amount+0.001
customer_freq = transaction_data[['CustomerCode', 'Amount']].groupby("CustomerCode").count().reset_index()
customer_freq.rename(columns={'Amount':'frequency'},inplace=True)
customer_history_df = customer_history_df.merge(customer_freq, how='outer')

#taking log of RFM Feature
customer_history_df['recency_log'] = customer_history_df['recency'].apply(np.log)
customer_history_df['frequency_log'] = customer_history_df['frequency'].apply(np.log)
customer_history_df['amount_log'] = customer_history_df['Amount'].apply(np.log)

#removing null
customer_history_df=customer_history_df.dropna()
#selecting feature
feature=['recency_log','frequency_log','amount_log']
X=customer_history_df[feature].values
#scaling the feature
scaler=MinMaxScaler()
scaler.fit(X)
X=scaler.transform(X)

#Conducting Agglomerative Clustering
clustering=AgglomerativeClustering(n_clusters=7).fit(X)
#Assigning Cluster Label
customer_history_df['Cluster_Label']=clustering.labels_
#Labeled Customer Cluster
customer_history_df['Cluster_Label'].replace({0:"TemporaryWalking",1:"TemporaryWalking",2:"NewEmerging",3:"PureLoyal",4:"ChurningLessImportant",5:"LoyalLessImportant",6:"Churning"},inplace=True)

#Storing Analytics Result  and Cluster Algorithm Output In Database

#Adding ResearchId to customer_history dataframe
customer_history_df['ResearchId']=research_id
df=customer_history_df[['ResearchId','CustomerCode','Cluster_Label','recency','frequency','Amount',]]
#rename dataframe columns name as CustomerAnalyticsResult schema columns name
df.rename(columns={'Cluster_Label':'CustomerCluster','recency':'Recency','frequency':'Frequency','Amount':'MonetaryValue'},inplace=True)
try:
    engine = sa.create_engine("mssql+pyodbc://sa:flexiload@192.168.11.206/RetailAI?driver=SQL+Server+Native+Client+11.0")
    print("Engine Created Successfully.")
except Exception as exp:
    print("Can not create engine")
    print(exp)

try:
    df.to_sql('CustomerAnalyticsResult',con=engine,if_exists='append',index=False)
    print("Data Write Back to Database Successfull.")
except Exception as exp:
    print("Data Write Back to Database Unsuccessfull.")
    print(exp)
