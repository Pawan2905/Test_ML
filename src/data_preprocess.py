import glob
import pandas as pd
import numpy as np
import re
from datetime import datetime


path = glob.glob("./logs/*.log")
# print(path)

def data_prep(path):
    ip_add= []
    hyphen_lst =[]
    cust_id = []
    date_request = []
    request=[]
    size_obj=[]
    response_lst = []
    url_lst = []
    for i in range(len(path)):
        print(i,path[i].replace("\\","/"))
        file = open(path[i].replace("\\","/"), 'r')
        # line = file.read().splitlines()
        # print(lines[0])
        lines= file.read()
        file.close()

        # print(lines[0])

        ip = re.findall( r'[0-9]+(?:\.[0-9]+){3}', lines )
        # print(ip)
        print(len(ip))
        ip_add.extend(ip)

        hyphen = re.findall(r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}.[^a-b]',lines)
        print(len(hyphen))
        hyphen_lst.extend(hyphen)

        num_date = re.findall(r'\d{1,5}\s\[\d{2}\/\w{3}\/\d{4}\:\d{2}\:\d{2}\:\d{2}\s\+\d{4}\]',lines)     #'\d+\s\[(\d{2}\/[A-Za-z]{3}\/\d{4}\:\d{2}\:\d{2}\:\d{2}\s(\+|\-)\d{4})\]',lines)
        # print(num_date)
        print("date",len(num_date))
        date_request.extend(num_date)


        line=lines.splitlines()
        print("# line",len(line))
        for j in range(len(line)):
            request.append(line[j][line[j].find("GET"):line[j].find("1.0")+len("1.0")])
            size_obj.append(line[j][line[j].find("1.0")+len("1.0"):line[j].find("http")])
        
            response_lst.append(line[j][line[j].find(".com")+len(".com"):])
        print("url",len(request))
        print(len(size_obj))
        # print(line[1][line[1].find("GET"):line[1].find("1.0")+len("1.0")])
        # print(line[1][line[1].find("1.0")+len("1.0"):line[1].find("http")])

        # print(line[1])

        url=re.findall(r'(https?://[^\s]+)', lines)
        url_lst.extend(url)
        print('url',len(url))

    print("ID",len(ip_add))
    print("hyp",len(hyphen_lst))
    print("cust",len(cust_id))
    print("date",len(date_request))
    print("url",len(url_lst))

    df = pd.DataFrame()

    # df['IP']= ip_add
    df['hyp']=hyphen_lst
    # df['cust_id']=cust_id
    df['request_date'] = date_request
    df['request'] = request
    df['size_obj']=size_obj
    df['url']=url_lst
    df['response'] = response_lst
    print(df.head())

    df[['IP_Address','Hyphen']] = df['hyp'].str.split(' ', 1, expand=True)
    df[['cust_id','date_time']]=df['request_date'].str.split(' ',1,expand=True)
    df['date_time']=df['date_time'].apply(lambda x:x[1:-1])
    df['date_time'] = df['date_time'].apply(lambda x:datetime.strptime(x, "%d/%b/%Y:%H:%M:%S %z"))
    df[['method','request_proto']]= df['request'].str.split('/', 1, expand=True)
    df[['client_request','client_protocol']]=df['request_proto'].str.split(' ',1,expand=True)
    df['size_obj']= df['size_obj'].apply(lambda x:x[1:-1].strip())
    df[['status_code','obj_size']]=df['size_obj'].str.split(" ",1,expand=True)
    df['response_code'] = df['response'].apply(lambda x:x[1:-1].strip().split()[-1] if len(x[1:-1].strip().split())>3 else "")
    df['response']=df['response'].apply(lambda x:x[1:-1].strip()[1:])

    df=df[['IP_Address',
        'Hyphen', 'cust_id', 'date_time', 'method','client_request', 'client_protocol', 'status_code', 'obj_size','response','response_code']]
    # df.to_csv("./output/agg_data1.csv",index=False)
    df.to_csv("./output/preproces.csv",index=False)
    return df

