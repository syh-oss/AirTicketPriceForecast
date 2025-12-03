from django.shortcuts import render,HttpResponse,redirect
from app01 import models
from app01.utils.pagination import Pagination
from app01.utils.form import UserModelForm,MyForm,PrettyModelForm,PrettyEditModelForm
# Create your views here.


def user_list(request):

    queryset= models.UserInfo.objects.all()
    # for obj in queryset:
        # print(obj.id,obj.name,obj.password,obj.get_gender_display(),obj.create_time.strftime("%Y-%m-%d"),obj.depart.title)

    page_object = Pagination(request, queryset)
    context = {
        'queryset': page_object.page_queryset,  # 分完页的数据
        'page_string': page_object.html()  # 页码
    }
    return render(request,'user_list.html',context)

def user_add(request):
    if request.method=='GET':
        queryset=models.Department.objects.all()
        content={
            'gender_choices':models.UserInfo.gender_choices,
            "queryset":queryset  # 读取的部门的数据
        }
        return render(request,'user_add.html',content)
    user=request.POST.get('user.py')
    password=request.POST.get("password")
    age=request.POST.get('age')
    account=request.POST.get('account')
    ctime=request.POST.get('ctime')
    gender_id=request.POST.get('gender')
    depart_id=request.POST.get('department')
    models.UserInfo.objects.create(name=user, password=password, age=age,account=account,create_time=ctime,depart_id=depart_id,gender=gender_id)
    return redirect('/user/list/')

def user_model_form_add(request):
    """添加用户，基于modelFOrm版本"""
    if request.method=='GET':
        form =UserModelForm()
        return render(request,'user_model_form_add.html',{'form':form})
    # 用户POST提交的数据，数据校验
    form = UserModelForm(data=request.POST)
    if form.is_valid():
        form.save()
        return redirect('/user/list/')
    #校验失败（在页面上显示错误信息）
    return render(request, 'user_model_form_add.html', {'form': form})

def user_model_form_add2(request):
    if request.method=='GET':
        form = MyForm()
        return render(request, 'user_model_form_add2.html', {'form': form})
    form=MyForm(data=request.POST)
    if form.is_valid():
        form.save()
        return redirect('/user/list/')
    return render(request, 'user_model_form_add2.html', {'form': form})

def user_delete(request,nid):
    models.UserInfo.objects.filter(id=nid).delete()
    return redirect('/user/list/')

def user_edit(request,nid):
    # 根据ID去数据库获取要编辑的那一行数据
    row_object = models.UserInfo.objects.filter(id=nid).first()
    if request.method=='GET':
        form = MyForm(instance=row_object)
        return render(request,'user_edit.html',{'form':form})

    form=MyForm(data=request.POST,instance=row_object)
    if form.is_valid():
        form.save()
        return redirect('/user/list/')
    return render(request, 'user_edit.html', {'form': form})