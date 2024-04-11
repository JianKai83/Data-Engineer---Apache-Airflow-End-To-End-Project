from loguru import logger
from sqlalchemy import create_engine, engine, update, MetaData
import pandas as pd
import re
from datetime import datetime 
import jieba
import numpy as np
import json

from dataflow.config import (
    MYSQL_DATA_USER,
    MYSQL_DATA_PASSWORD,
    MYSQL_DATA_HOST,
    MYSQL_DATA_PORT,
    MYSQL_DATA_DATABASE,
)


def get_mysql_jobdata_conn() -> engine.base.Connection:
    address = (
        f"mysql+pymysql://{MYSQL_DATA_USER}:{MYSQL_DATA_PASSWORD}"
        f"@{MYSQL_DATA_HOST}:{MYSQL_DATA_PORT}/{MYSQL_DATA_DATABASE}"
    )
    engine = create_engine(address, encoding='utf-8')
    connect = engine.connect()
    return connect

def Update_Chart():
    mysql_conn = get_mysql_jobdata_conn()
    
    sql_104 = "select * from jobdata.job_104"
    df_104 = pd.read_sql(sql_104, con=mysql_conn)
    
    sql_1111 = "select * from jobdata.job_1111"
    df_1111 = pd.read_sql(sql_1111,con=mysql_conn)
    
    df_104 = df_104[['company','title','appearDate','addressArea','workExp','specialty']]
    df_1111 = df_1111[['company','title','appearDate','addressArea','workExp','specialty']]
    
    df = pd.merge(df_104, df_1111, how='outer')
    df = df.drop_duplicates()
    
    # work location
    character_set = {'基隆','台北','新北','桃園','新竹','宜蘭','苗栗','台中','彰化','南投','雲林','嘉義','台南','高雄','屏東','澎湖','花蓮','台東'}
    pattern = '|'.join(re.escape(char) for char in character_set)
    total_counts = df['addressArea'].apply(lambda x: {char: x.count(char) for char in re.findall(pattern, x)})

    address_list = ['基隆','台北','新北','桃園','新竹','宜蘭','苗栗','台中','彰化','南投','雲林','嘉義','台南','高雄','屏東','澎湖','花蓮','台東']
    result_dict = {i:0 for i in address_list}

    for counts in total_counts:
        for char, count in counts.items():
            result_dict[char] = result_dict.get(char, 0) + count
    
    areaCount = {'北部':result_dict['基隆']+result_dict['台北']+result_dict['新北']+result_dict['桃園']+result_dict['新竹']+result_dict['宜蘭'],
                '中部':result_dict['苗栗']+result_dict['台中']+result_dict['彰化']+result_dict['南投']+result_dict['雲林'],
                '南部':result_dict['嘉義']+result_dict['台南']+result_dict['高雄']+result_dict['屏東']+result_dict['澎湖'],
                '東部':result_dict['花蓮']+result_dict['台東']}
    
    loc_column = ['北部', '中部', '南部', '東部']
    loc_value = [areaCount['北部'], areaCount['中部'], areaCount['南部'], areaCount['東部']]


    # work experinece
    work_exp = df['workExp']
    work_exp = work_exp.str.replace('以上工作經驗','')
    work_exp = work_exp.str.replace('以上','')
    exp_column = list(work_exp.value_counts().index)
    exp_values = list(work_exp.value_counts().values)
    exp_values = [int(i) for i in exp_values]


    # update date
    today_date = datetime.now()
    update_value_counts = df['appearDate'].value_counts()
    update_dict = update_value_counts.to_dict()
    update_date = [datetime.strptime(str(i), '%Y-%m-%d') for i in update_dict]
    update_date_filtered = [i for i in update_date if (today_date - i).days <= 30]
    update_filtered_index = [i for i in update_dict if datetime.strptime(str(i), '%Y-%m-%d') in update_date_filtered]
    update_filtered_index.sort()
    update_cleaned_values = [update_dict[i] for i in update_filtered_index]
    update_filtered_index = [i.strftime('%m/%d') for i in update_filtered_index]

    # wordcloud
    jieba.load_userdict('./dataflow/analysis/custom_vocab.txt')
    del_words_path = './dataflow/analysis/delete_words.txt'
    tool = df['specialty']

    tool_str=''
    for i in tool:
        tool_str = tool_str + str(i) + '。'

    all_str = tool_str
    all_str = all_str.replace('MS SQL','MSSQL')
    all_str = all_str.replace('POWER BI','POWERBI')
    
    words = jieba.cut(all_str, cut_all=False)
    word_list = list(words)
    word_count = {}

    for word in word_list:
        if word in word_count:
            word_count[word] += 1
        else:
            word_count[word] = 1
            
    del_words_list = []

    with open(del_words_path, 'r', encoding='utf-8') as f:
        for line in f.readlines():
            del_words_list.append(line.replace('\n',''))
    
    word_list = []
    for k in word_count:
        if len(k) > 1 and k not in del_words_list:
            word_list.append((k,word_count[k]))
    
    val_list=[i[1] for i in word_list]
    val_set = list(set(val_list))
    word_list_percentile = [word_list[i] for i in range(0,len(word_list)) if word_list[i][1] > np.percentile(val_set,25)]
    word_list_percentile.sort(key= lambda x: x[1], reverse=True)
    

    word_list_dict = dict(word_list_percentile)
    wordcloud_list = [{'x': k, 'value': v} for k,v in word_list_dict.items()]
    print(wordcloud_list)
    
    # update database
    loc_column = json.dumps(loc_column)
    loc_value = json.dumps(loc_value)
    exp_column = json.dumps(exp_column)
    exp_values = json.dumps(exp_values)
    update_filtered_index = json.dumps(update_filtered_index)
    update_cleaned_values = json.dumps(update_cleaned_values)
    wordcloud_list = json.dumps(wordcloud_list)

    meta=MetaData(bind=mysql_conn)
    MetaData.reflect(meta)
    dashboard = meta.tables['dashboard_chart']
    u=update(dashboard)
    u = u.values({"location_column" : loc_column,
         "location_value" : loc_value,
         "exp_column" : exp_column,
        "exp_values" : exp_values,
        "update_column" : update_filtered_index,
        "update_values" : update_cleaned_values,
        "wordcloud" : wordcloud_list})
    u=u.where(dashboard.c.id==1)
    mysql_conn.execute(u)

    mysql_conn.close()

if __name__ == "__main__":
    Update_Chart()