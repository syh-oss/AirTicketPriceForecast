from datetime import timedelta
from django.http import HttpResponse
from sklearn.linear_model import BayesianRidge
import pandas as pd
import numpy as np
import json
from django.shortcuts import render,HttpResponse,redirect
from django.views.decorators.csrf import csrf_exempt
from app01 import models
from app01.utils.bootstrapwidget import BootStrapModelForm
from app01.utils.pagination import Pagination
import mysql.connector
import warnings
warnings.filterwarnings('ignore')
# 连接到 MySQL 数据库
conn = mysql.connector.connect(
    host="localhost",
    user="root",
    password="777777",
    database="gx_day16"
)
def predict_list(request):
    return render(request,'predict_main.html')

def predict_main(request):
    if request.method == 'POST':
        # 获取用户输入的特征值
        time_diff_str = int(request.POST.get('time_diff_str'))
        s_hour = int(request.POST.get('s_hour'))
        s_min = int(request.POST.get('s_min'))
        d_hour = int(request.POST.get('d_hour'))
        d_min = int(request.POST.get('d_min'))
        date_m = int(request.POST.get('date_m'))
        date_d = int(request.POST.get('date_d'))
        company_encoded = int(request.POST.get('company_encoded'))
        sPlace_encoded = int(request.POST.get('sPlace_encoded'))
        dPlace_encoded = int(request.POST.get('dPlace_encoded'))
        # 构建特征列表
        ticket_features = [[time_diff_str, s_hour, s_min, d_hour, d_min, date_m, date_d, company_encoded, sPlace_encoded, dPlace_encoded]]
        # 读取机票数据文件
        # 执行 SQL 查询并将结果读取为 DataFrame
        query = "SELECT * FROM qunaer"
        df = pd.read_sql(query, conn)
        #df = pd.read_csv('qunaer1.csv', header=None, index_col=0)
        df.columns = ['id','DepAndDest', 'date', 'company','type','sPlace','sTime','dTime','dPlace','price','Urls']
        df['date'] = df['date'].apply(lambda x: pd.to_datetime(x, format='%m-%d').replace(year=2024).strftime('%Y/%m/%d'))
        df['sTime'] = pd.to_datetime(df['date'], format='%Y/%m/%d') + pd.to_timedelta(df['sTime'] + ':00')
        if "+1天" in df["dTime"]:
            df['dTime'] = pd.to_datetime(df['date'], format='%Y/%m/%d') + pd.to_timedelta(df['dTime'].apply(lambda x: x.split(' ')[0] + ':00'))+ timedelta(days=1)
        else:
            df['dTime'] = pd.to_datetime(df['date'], format='%Y/%m/%d') + pd.to_timedelta(df['dTime'] + ':00')
        df['duration'] = df['dTime'] - df['sTime']
        df['time_difference_minutes'] = df['duration'].dt.total_seconds() // 60
        negative_days_mask = df['duration'] < pd.Timedelta(0)
        df.loc[negative_days_mask, 'time_difference_minutes'] += df.loc[negative_days_mask, 'duration'].apply(lambda x: -x.days * 24 * 60)
        df['company_encoded'] = pd.factorize(df['company'])[0]
        df['sPlace_encoded'] = pd.factorize(df['sPlace'])[0]
        df['dPlace_encoded'] = pd.factorize(df['dPlace'])[0]
        df.drop(['company','sPlace','dPlace'], axis = 1, inplace = True)
        df["date"]=pd.to_datetime(df["date"])
        df['date_m'] = df['date'].dt.month
        df['date_d'] = df['date'].dt.day
        df.drop(['date'], axis = 1, inplace = True)
        df["d_hour"] = pd.to_datetime(df["dTime"]).dt.hour
        df["d_min"] = pd.to_datetime(df["dTime"]).dt.minute
        df.drop(["dTime"], axis = 1, inplace = True)
        df["s_hour"] = pd.to_datetime(df["sTime"]).dt.hour
        df["s_min"] = pd.to_datetime(df["sTime"]).dt.minute
        df.drop(["sTime"], axis = 1, inplace = True)
        df.drop(["duration"], axis = 1, inplace = True)
        # 提取特征和目标变量
        X = df[['time_difference_minutes', 's_hour', 's_min', 'd_hour', 'd_min', 'date_m', 'date_d', 'company_encoded', 'sPlace_encoded', 'dPlace_encoded']]
        y = df['price']
        # 初始化并训练贝叶斯线性回归模型
        bayesian_model = BayesianRidge()
        bayesian_model.fit(X, y)
        # 进行预测
        ticket_price_prediction = bayesian_model.predict(ticket_features)
        # 返回预测结果给模板
        return render(request, 'predict_main.html', {'predicted_price': ticket_price_prediction[0]})
    else:
        # 如果不是POST请求，返回空白表单
        return render(request, 'predict_main.html', {'predicted_price': None})
