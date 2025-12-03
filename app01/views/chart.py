import json
import random
from datetime import datetime
from django.shortcuts import render, HttpResponse, redirect
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from app01 import models
from app01.utils.bootstrapwidget import BootStrapModelForm
from app01.utils.pagination import Pagination
import pandas as pd
import os

script_path = os.path.abspath(__file__)
# 构造 CSV 文件的相对路径
csv_path = os.path.join(os.path.dirname(script_path), 'qunaer.csv')
df = pd.read_csv(csv_path, header=None, index_col=0)
df.columns = ['s_to_d', 'date', 'company', 'type', 'sPlace', 'sTime', 'dTime', 'dPlace', 'price', 'from']
df1 = df[['date', 'price']]
df2 = df1.groupby('date')['price'].mean()
df3 = df2.reset_index()
df['month'] = df['date'].str.slice(stop=-3)
result_qunaer = df.groupby(df['month']).size().tolist()
counts = df['company'].value_counts().sort_values(ascending=False)
company = counts.index.tolist()
counts_list = counts.tolist()

csv_path2 = os.path.join(os.path.dirname(script_path), 'tongcheng.csv')
df_tong = pd.read_csv(csv_path2, header=None, index_col=0)
df_tong.columns = ['s_to_d', 'date', 'company', 'type', 'sPlace', 'sTime', 'dTime', 'dPlace', 'price', 'from']
df_tong['date'] = df_tong['date'].str.slice(stop=-2)
idx = df_tong.loc[df_tong['date'] == '07-11 '].index[0]
df_tong = df_tong.loc[:(idx - 1)]
df_tong1 = df_tong[['date', 'price']]
df_tong2 = df_tong1.groupby('date')['price'].mean()
df_tong3 = df_tong2.reset_index()
df_tong['month'] = df_tong['date'].str.slice(stop=-4)
result_tong = df_tong.groupby(df_tong['month']).size().tolist()

csv_path3 = os.path.join(os.path.dirname(script_path), 'xiecheng.csv')
df_xie = pd.read_csv(csv_path3, header=None, index_col=0)
df_xie.columns = ['s_to_d', 'date', 'company', 'type', 'sPlace', 'sTime', 'dTime', 'dPlace', 'price', 'from']
df_xie['date1'] = pd.to_datetime(df_xie['date'], format='%m月%d日')
df_xie = df_xie[df_xie['date1'] <= '1900-07-10']
start_date = '1900-05-01'
end_date = '1900-07-10'
dates = pd.date_range(start_date, end_date)
df_all = pd.DataFrame(index=dates)
df_xie.set_index('date1', inplace=True)
df_all = df_all.merge(df_xie, how='left', left_index=True, right_index=True)
df_all.fillna(0, inplace=True)
df_all['date_str'] = df_all.index.month.map('{:02d}'.format) + '月' + df_all.index.day.map('{:02d}'.format) + '日'
df_all1 = df_all[['date_str', 'price']]
df_all2 = df_all1.groupby('date_str')['price'].mean()
df_all3 = df_all2.reset_index()
df_xie['date1'] = pd.to_datetime(df_xie['date'], format='%m月%d日')
result_xie = df_xie.groupby(df_xie['date1'].dt.month).size().tolist()




def chart_list(request):
    """数据统计页面"""
    return render(request, 'chart_list.html')


def chart_bar(request):
    """构造柱状图的数据"""
    legend = ['去哪儿', '同程','协程']

    series_list = [
        {
            'name': '去哪儿',
            'type': 'bar',
            'data': result_qunaer
        },
        {
            'name': '同程',
            'type': 'bar',
            'data': result_tong
        },
        {
            'name': '协程',
            'type': 'bar',
            'data': result_xie
        }
    ]

    x_axis = ['5月', '6月', '7月']

    result = {
        'status': True,
        'data': {
            'legend': legend,
            'series_list': series_list,
            'x_axis': x_axis
        }
    }
    return JsonResponse(result)


def chart_pie(request):
    """构造饼图的数据"""

    series_list = []

    for i in range(len(company)):
        series_list.append({'value': counts_list[i], 'name': company[i]})

    result = {
        'status': True,
        'data': series_list
    }
    return JsonResponse(result)


def chart_line(request):
    """构造柱状图的数据"""
    # 获取当前脚本的绝对路径

    legend1 = ['去哪儿', '同程', '协程']
    series_list = [

        {
            'name': '协程网站',
            'type': 'line',
            'stack': 'Total',
            'data': df_all3['price'].to_list()
        },
        {
            'name': '去哪儿网站',
            'type': 'line',
            'stack': 'Total',
            'data': df3['price'].to_list()
        },
        {
            'name': '同程网站',
            'type': 'line',
            'stack': 'Total',
            'data': df_tong3['price'].to_list()
        },

    ]

    x_axis = df3['date'].to_list()

    result = {
        'status': True,
        'data': {
            'legend1': legend1,
            'series_list': series_list,
            'x_axis': x_axis
        }
    }
    return JsonResponse(result)


if __name__ == '__main__':
    chart_line()
