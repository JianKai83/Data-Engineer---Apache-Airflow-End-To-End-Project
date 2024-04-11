from flask import Flask, render_template, request, jsonify, flash
import pandas as pd
import pymysql
from loguru import logger
from sqlalchemy import create_engine, engine,text
import json

from api import config

def get_mysql_jobdata_conn() -> engine.base.Connection:
    address = (
        f"mysql+pymysql://{config.MYSQL_DATA_USER}:{config.MYSQL_DATA_PASSWORD}"
        f"@{config.MYSQL_DATA_HOST}:{config.MYSQL_DATA_PORT}/{config.MYSQL_DATA_DATABASE}"
    )
    engine = create_engine(address, encoding='utf-8')
    connect = engine.connect()
    return connect

# read data
sql_104 = "select * from jobdata.job_104"
sql_1111 = "select * from jobdata.job_1111"
mysql_conn = get_mysql_jobdata_conn()
df_104 = pd.read_sql(sql_104, con=mysql_conn)
df_1111 = pd.read_sql(sql_1111, con=mysql_conn)
mysql_conn.close()

# 將資料轉換成列表
data_list_104 = df_104.to_dict(orient='records')
data_list_1111 = df_1111.to_dict(orient='records')

app = Flask(__name__)

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/wordcloud')
def wordcloud():
    sql = "select * from jobdata.dashboard_chart"
    mysql_conn = get_mysql_jobdata_conn()
    df = pd.read_sql(sql, con=mysql_conn)
    mysql_conn.close()

    loc_column = df.iloc[0]['location_column']
    loc_value = df.iloc[0]['location_value']
    exp_column = df.iloc[0]['exp_column']
    exp_values = df.iloc[0]['exp_values']
    update_filtered_index = df.iloc[0]['update_column']
    update_cleaned_values = df.iloc[0]['update_values']
    # wordcloud 需要先 loads 回來才可以傳
    wordcloud_list = df.iloc[0]['wordcloud']
    wordcloud_list = json.loads(wordcloud_list)
    
    return render_template('jobDashboard.html'
                           ,loc_column=loc_column, loc_value=loc_value
                           ,exp_column = exp_column, exp_values = exp_values
                           ,update_column = update_filtered_index, update_values = update_cleaned_values
                           ,wordcloud=wordcloud_list)

@app.route('/job104')
def job104():
    return render_template('data104_v2.html', data=data_list_104[0], current_index=0)

@app.route('/navigate104', methods=['POST'])
def navigate104():
    current_index = int(request.form['current_index'])
    direction = request.form['direction']

    if direction == 'prev':
        next_index = max(current_index - 1, 0)
    elif direction == 'next':
        next_index = min(current_index + 1, len(data_list_104) - 1)
    else:
        next_index = current_index

    return render_template('data104_v2.html', data=data_list_104[next_index], current_index=next_index)

@app.route('/job1111')
def job1111():
    return render_template('data1111_v2.html', data=data_list_1111[0], current_index=0)

@app.route('/navigate1111', methods=['POST'])
def navigate1111():
    current_index = int(request.form['current_index'])
    direction = request.form['direction']

    if direction == 'prev':
        next_index = max(current_index - 1, 0)
    elif direction == 'next':
        next_index = min(current_index + 1, len(data_list_1111) - 1)
    else:
        next_index = current_index

    return render_template('data1111_v2.html', data=data_list_1111[next_index], current_index=next_index)

if __name__ == '__main__':
    app.run(host="0.0.0.0", port=5000, debug=True)