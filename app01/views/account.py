from django.shortcuts import render, HttpResponse, redirect
from app01 import models
from app01.utils.pagination import Pagination
from app01.utils.form import AdminModelForm, AdminEditModelForm, AdminResetModelForm
from django import forms
from app01.utils.bootstrapwidget import BootStrapModelForm, BootStrapForm
from app01.utils.encrypt import md5
from app01.utils.code import check_code
import django
class LoginForm(BootStrapForm):
    username = forms.CharField(label="用户名", widget=forms.TextInput, required=True)
    # render_value=True 当数据用post提交后，仍保留在网页上
    password = forms.CharField(label="密码", widget=forms.PasswordInput(render_value=True), required=True)
    # code = forms.CharField(label='验证码', widget=forms.TextInput, required=True)

    # 定义钩子函数实现对password加密
    def clean_password(self):
        pwd = self.cleaned_data.get("password")
        return md5(pwd)
# 用modelform和用form效果一样
# class LoginModelForm(forms.ModelForm):
#     class Meta:
#         model = models.Admin
#         fields = ['username','password']

def login(request):     #默认需要参数request
    """登录"""
    if request.method == "GET":
        form = LoginForm()
        return render(request, 'login.html', {"form": form})
    form = LoginForm(data=request.POST)
    if form.is_valid():
        # 验证成功，获取到的用户名和密码
        # print(form.cleaned_data)
        # 验证码的校验
        # user_input_code = form.cleaned_data.pop('code')
        # code = request.session.get('image_code', '')
        # if code.upper() != user_input_code.upper():
        #     form.add_error('code', '验证码错误')
        #     return render(request, 'login.html', {'form': form})
        # 取数据库校验用户名和密码是否正确，获取用户对象或none
        # admin_object = models.Admin.objects.filter(username =form.cleaned_data['username'],password =form.cleaned_data['password'] ).first()
        admin_object = models.Admin.objects.filter(**form.cleaned_data).first()
        if not admin_object:
            # 在网页上显示错误信息
            form.add_error("password", "用户名或密码错误")
            return render(request, 'login.html', {'form': form})

        # 用户名和密码输入正确,网站生产随机字符串，写到用户浏览器的cookie中，写到session中。
        request.session['info'] = {'id': admin_object.id, "name": admin_object.username}
        request.session.set_expiry(60 * 60 * 24 * 7)  # 登录成功后，设置7天免登录
        return redirect('/pnum/list/')
    return render(request, 'login.html', {'form': form})

def logout(request):
    """ 注销 """
    request.session.clear()
    return redirect("/login/")


