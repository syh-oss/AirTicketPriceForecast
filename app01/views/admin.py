from django.shortcuts import render,HttpResponse,redirect
from app01 import models
from app01.utils.pagination import Pagination
from app01.utils.form import AdminModelForm,AdminEditModelForm,AdminResetModelForm

def admin_list(request):
    """管理员列表"""
    info = request.session.get('info')
    if not info:
        return redirect('/login/')

    #搜索
    data_dict = {}
    search_data = request.GET.get('q', "")
    if search_data:
        data_dict['username__contains'] = search_data

    # 根据搜索条件去数据库获取
    queryset = models.Admin.objects.filter(**data_dict)


    #分页
    page_object = Pagination(request, queryset)
    context = {
        'queryset': page_object.page_queryset,  # 分完页的数据
        'page_string': page_object.html(),  # 生成页码
        'search_data':search_data  #把搜索关键字传回到admin_list.html中input的value中
    }
    return render(request, 'admin_list.html', context)


def admin_add(request):
    """添加管理员"""
    title= "新建管理员"

    if request.method =='GET':
        form = AdminModelForm()
        return render(request,'add.html', {'title':title,'form':form})
    form=AdminModelForm(data=request.POST)
    if form.is_valid():
        form.save()
        return redirect('/admin/list')
    return render(request, 'add.html', {'title':title,'form':form})

def admin_edit(request,nid):
    title = "编辑管理员"
    # 根据ID去数据库获取要编辑的那一行数据
    row_object = models.Admin.objects.filter(id=nid).first()
    if not row_object:
        return render(request,'error.html')
    if request.method=='GET':
        form = AdminEditModelForm(instance=row_object)
        return render(request,'add.html',{'form':form,'title':title})

    form = AdminEditModelForm(data=request.POST,instance=row_object)
    if form.is_valid():
        form.save()
        return redirect('/admin/list/')
    return render(request, 'add.html', {'form': form})

def admin_delete(request,nid):
    models.Admin.objects.filter(id=nid).delete()
    return  redirect('/admin/list/')


def admin_reset(request,nid):
    # 根据ID去数据库获取要编辑的那一行数据
    row_object = models.Admin.objects.filter(id=nid).first()
    if not row_object:
        return render(request,'error.html')

    title = "编辑管理员 - {}".format(row_object.username)

    if request.method == 'GET':
        form = AdminResetModelForm()  #为了不让人看到数据库记录的密码，在括号内不写instance=row_object
        return render(request, 'add.html', { 'form': form,'title': title})
    form = AdminResetModelForm(data=request.POST,instance=row_object)
    if form.is_valid():
        form.save()
        return redirect('/admin/list')
    return render(request, 'add.html', {'title': title, 'form': form})