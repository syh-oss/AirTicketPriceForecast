from django.shortcuts import render,HttpResponse,redirect
from app01 import models
from app01.utils.pagination import Pagination
from app01.utils.form import UserModelForm,MyForm,PrettyModelForm,PrettyEditModelForm
# Create your views here.

def pnum_list(request):
    """机票列表"""

    data_dict={}
    search_data = request.GET.get('q',"")
    if search_data:
        data_dict['time_day__contains'] = search_data
    queryset = models.FlightTicket.objects.filter(**data_dict).order_by("time_day",'start_time')

    page_object = Pagination(request,queryset)
    context={
        "search_data": search_data,

        'queryset':page_object.page_queryset,   #分完页的数据
        'page_string':page_object.html()   #页码
             }
    return render(request,'pnum_list.html',context)

