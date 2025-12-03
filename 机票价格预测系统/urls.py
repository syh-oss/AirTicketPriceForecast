"""机票价格预测系统 URL Configuration

The `urlpatterns` list routes URLs to views. For more information please see:
    https://docs.djangoproject.com/en/2.2/topics/http/urls/
Examples:
Function views
    1. Add an import:  from my_app import views
    2. Add a URL to urlpatterns:  path('', views.home, name='home')
Class-based views
    1. Add an import:  from other_app.views import Home
    2. Add a URL to urlpatterns:  path('', Home.as_view(), name='home')
Including another URLconf
    1. Import the include() function: from django.urls import include, path
    2. Add a URL to urlpatterns:  path('blog/', include('blog.urls'))
"""
from django.contrib import admin
from django.urls import path, re_path
from django.views.static import serve
from django.conf import settings
from app01 import views
from app01.views import  pretty, admin, account,  order, chart,predict
from django.shortcuts import render, HttpResponse, redirect
urlpatterns = [
    # re_path(r'^media/(?P<path>.*)$',serve,{'document_root':settings.MEDIA_ROOT},name='media'),
    path('', lambda request: redirect('login/', permanent=True)),
    # path('admin/', admin.site.urls),

    # '''靓号管理'''
    path('pnum/list/', pretty.pnum_list),

    # '''管理员账户'''
    path('admin/list/', admin.admin_list),
    path('admin/add/', admin.admin_add),
    path('admin/<int:nid>/edit/', admin.admin_edit),
    path('admin/<int:nid>/delete/', admin.admin_delete),
    path('admin/<int:nid>/reset/', admin.admin_reset),

    # """ 登录"""
    path('login/', account.login),
    path('logout/', account.logout),
    # path('image/code/', account.image_code),


    # """ 订单管理"""
    path('order/list/', order.order_list),
    path('order/add/', order.order_add),
    path('order/delete/', order.order_delete),
    path('order/detail/', order.order_detail),
    path('order/edit/', order.order_edit),

    # """ 数据统计"""
    path('chart/list/', chart.chart_list),
    path('chart/bar/', chart.chart_bar),
    path('chart/pie/', chart.chart_pie),
    path('chart/line/', chart.chart_line),

    # """ 预测功能"""
    path('predict/main/', predict.predict_main),
    path('predict/list/', predict.predict_list),

]

