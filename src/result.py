import glob
import numpy as np
import pandas as pd
import data_preprocess as dp

path = glob.glob("./logs/*.log")
if __name__ == "__main__":
  df_preprocessed=dp.data_prep(path)
  df_preprocessed['success_flag']= df_preprocessed['response_code'].apply(lambda x:1 if x.startswith("2") else 0)

  unique_cust = df_preprocessed['cust_id'].nunique()

  sucess_request=df_preprocessed.loc[df_preprocessed['success_flag']==1,'success_flag'].count()
  print('success_request  ',sucess_request)

  user_request=df_preprocessed.groupby(['cust_id'])['client_request'].count().sort_values(ascending=False).reset_index()

  writer = pd.ExcelWriter('./output/output_res.xlsx',engine='xlsxwriter')

  names = ['number of successful request','request_per_user']

  df_cust = pd.DataFrame()
  df_cust['Number_unique_customer']= [unique_cust]
  df_cust['Number_of_successful_request'] = [sucess_request]

  dataframes = [df_cust,user_request]

  for i,frame in enumerate(dataframes):
    print(i)
    frame.to_excel(writer,sheet_name=names[i],index=False)
  writer.save()
  writer.close()

