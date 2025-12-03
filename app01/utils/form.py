
from django.core.validators import RegexValidator
from django.core.exceptions import ValidationError
from app01 import models
from django import forms
from app01.utils.bootstrapwidget import BootStrapModelForm,BootStrapForm
from app01.utils.encrypt import md5


class UserModelForm(BootStrapModelForm):
    name = forms.CharField(min_length=3,label="用户名")
    class Meta:
        model = models.UserInfo
        fields = ['name','password','age','account','create_time','gender','depart']

class MyForm(BootStrapModelForm):
    class Meta:
        model = models.UserInfo
        fields=['name','password','age','account','create_time','depart','gender']

class PrettyModelForm(BootStrapModelForm):
    mobile = forms.CharField(
        label="手机号",
        validators = [RegexValidator(r'^1[3-9]\d{9}$', '手机号格式错误'),],
        )

    class Meta:
        model = models.PrettyNum
        # fields = ['mobile','price','level','status']
        fields = "__all__"
        # exclude=['level']



    # 验证：方式2
    def clean_mobile(self):

        txt_mobile = self.cleaned_data['mobile']
        exists = models.PrettyNum.objects.filter(mobile=txt_mobile).exists()
        if exists:
            # 验证不通过
            raise ValidationError("手机号已存在")
        # 验证通过，用户输入的值返回
        return txt_mobile

class PrettyEditModelForm(BootStrapModelForm):
    # mobile = forms.CharField(disabled=True,label="手机号")
    class Meta:
        model = models.PrettyNum
        # fields = ['price','level','status']
        fields = "__all__"
        # exclude=['level']


    def clean_mobile(self):

        # 编辑哪一行的ID ：self.instance.pk
        txt_mobile = self.cleaned_data['mobile']
        exists = models.PrettyNum.objects.exclude(id=self.instance.pk).filter(mobile=txt_mobile).exists()
        if exists:
            # 验证不通过
            raise ValidationError("手机号已存在")
        # 验证通过，用户输入的值返回
        return txt_mobile

class AdminModelForm(BootStrapModelForm):
    confirm_password = forms.CharField(label='确认密码',widget=forms.PasswordInput(render_value=True))
    class Meta:
        model=models.Admin
        fields = ['username','password','confirm_password']
        widgets= {
            "password":forms.PasswordInput(render_value=True),
        }

    def clean_password(self):
        pwd = self.cleaned_data.get('password')
        return md5(pwd)

    def clean_confirm_password(self): #clean_字段名，将来告警也会出现在字段名旁边
        print(self.cleaned_data)
        pwd = self.cleaned_data.get('password')
        confirm = md5(self.cleaned_data.get('confirm_password'))
        if confirm !=pwd:
            raise ValidationError('密码不一致，请重新输入')
        # 返回什么，此字段以后保存到数据库就是什么
        return confirm

class AdminEditModelForm(BootStrapModelForm):
    class Meta:
        model = models.Admin
        fields = ['username']



class AdminResetModelForm(BootStrapModelForm):
    confirm_password = forms.CharField(label='确认密码', widget=forms.PasswordInput(render_value=True))
    class Meta:
        model = models.Admin
        fields = ['password','confirm_password']
        widgets = {
            "password": forms.PasswordInput(render_value=True),
        }

    def clean_password(self):
        pwd = self.cleaned_data.get('password')
        md5_pwd= md5(pwd)
        # 去数据库校验当前密码和新输入密码是否一致（新输入的数据在self.instance.pk中）
        exists=models.Admin.objects.filter(id = self.instance.pk,password =md5_pwd).exists()
        if exists:
            raise ValidationError('不能和以前的密码一致')

        return md5_pwd

    def clean_confirm_password(self):  # clean_字段名，将来告警也会出现在字段名旁边
        print(self.cleaned_data)
        pwd = self.cleaned_data.get('password')
        confirm = md5(self.cleaned_data.get('confirm_password'))
        if confirm != pwd:
            raise ValidationError('密码不一致，请重新输入')
        # 返回什么，此字段以后保存到数据库就是什么
        return confirm
