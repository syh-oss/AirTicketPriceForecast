import json
import random
from datetime import datetime
from django.shortcuts import render, HttpResponse, redirect
from django.views.decorators.csrf import csrf_exempt
from app01 import models
from app01.utils.bootstrapwidget import BootStrapModelForm
from app01.utils.pagination import Pagination


class OrderModeForm(BootStrapModelForm):
    class Meta:
        model = models.Order
        fields = "__all__"
        # fields=['title','price','status','admin']
        exclude = ['oid', 'admin']


def order_list(request):
    """任务列表"""
    # 去数据库获取所有的任务
    queryset = models.Order.objects.all().order_by('-id')
    page_object = Pagination(request, queryset)
    form = OrderModeForm()
    content = {
        "form": form,
        "queryset": page_object.page_queryset,
        'page_string': page_object.html()  # 页码
    }
    return render(request, 'order_list.html', content)


@csrf_exempt
def order_add(request):
    """新增订单（ajax）"""
    form = OrderModeForm(data=request.POST)
    if form.is_valid():
        # 在前端没有要求输入oid，可以在这里设置
        form.instance.oid = datetime.now().strftime('%Y%m%d%H%M%S') + str(random.randint(1000, 9999))
        # 设置登录者的id为管理员id
        form.instance.admin_id = request.session['info']['id']
        form.save()
        data_dict = {'status': True}
        return HttpResponse(json.dumps(data_dict))

    data_dict = {'status': False, 'error': form.errors}
    return HttpResponse(json.dumps(data_dict, ensure_ascii=False))


def order_delete(request):
    """删除部门"""
    nid = request.GET.get('uid')
    exists = models.Order.objects.filter(id=nid).exists()
    if not exists:
        return HttpResponse(json.dumps({'status': False, 'error': '数据不存在'}))
    models.Order.objects.filter(id=nid).delete()
    return HttpResponse(json.dumps({'status': True}))
    # return redirect('/order/list/')


def order_detail(request):
    """根据id获取订单信息"""
    nid = request.GET.get('uid')
    # row_object = models.Order.objects.filter(id=nid).first()
    row_dict = models.Order.objects.filter(id=nid).values('title', 'price', 'status').first()
    if not row_dict:
        return HttpResponse(json.dumps({'status': False, 'error': '数据不存在'}))

    # 从数据库中获取一个对象row_object
    result = {
        "status": True,
        "data": row_dict,
    }
    return HttpResponse(json.dumps(result))

@csrf_exempt
def order_edit(request):
    """编辑订单"""
    uid=request.GET.get('uid')
    row_object = models.Order.objects.filter(id=uid).first()
    if not row_object:
        return HttpResponse(json.dumps({'status': False, 'error': '数据不存在'}))

    form = OrderModeForm(data = request.POST, instance = row_object)
    if form.is_valid():
        form.save()
        return HttpResponse(json.dumps({'status': True}))

    return HttpResponse(json.dumps({'status': False, 'error': form.errors}))
