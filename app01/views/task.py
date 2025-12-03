import json
from django.shortcuts import render,HttpResponse,redirect
from django.views.decorators.csrf import csrf_exempt
from app01 import models
from app01.utils.bootstrapwidget import BootStrapModelForm
from app01.utils.pagination import Pagination

class TaskModeForm(BootStrapModelForm):
    class Meta:
        model = models.Task
        fields = "__all__"

def task_list(request):
    """任务列表"""
    # 去数据库获取所有的任务
    queryset = models.Task.objects.all().order_by('-id')
    page_object = Pagination(request, queryset, page_size=8)
    form = TaskModeForm()
    content = {
        "form": form,
        "queryset":page_object.page_queryset,
        'page_string': page_object.html()  # 页码
    }
    return render(request,'task_list.html',content)

@csrf_exempt
def task_ajax(request):
    """任务测试"""
    # print(request.GET)
    print(request.POST)
    data_dict = {'status':True,'data':[11,22,33,44]}
    return HttpResponse(json.dumps(data_dict))

@csrf_exempt
def task_add(request):
    print(request.POST)

    # 1.用户发送过来的数据进行校验（ModelForm进行校验）
    form = TaskModeForm(data=request.POST)
    if form.is_valid():
        form.save()
        data_dict = {'status': True}
        return HttpResponse(json.dumps(data_dict))

    data_dict = {'status':False,'error':form.errors}
    return HttpResponse(json.dumps(data_dict,ensure_ascii=False))