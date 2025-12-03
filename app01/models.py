from datetime import datetime
from django.db import models
# Create your models here.
class Admin(models.Model):
    '''管理员'''
    username = models.CharField(verbose_name='用户名', max_length=32)
    password = models.CharField(verbose_name='密码', max_length=64)

    def __str__(self):
        return self.username

class Department(models.Model):
    """部门表"""
    title=models.CharField(verbose_name='标题',max_length=32)
    def __str__(self):
        return self.title

class UserInfo(models.Model):
    """员工表"""
    name = models.CharField(verbose_name='姓名',max_length=16)
    password = models.CharField(verbose_name='密码',max_length=64)
    age = models.IntegerField(verbose_name='年龄')
    account = models.DecimalField(verbose_name='账户余额',max_digits=10,decimal_places=2,default=0)
    # create_time = models.DateTimeField(verbose_name='入职时间')
    create_time = models.DateField(verbose_name='入职时间')

    # 写的depart，生成数据列depart_id, to 与数据表关联，to_field 与表中的那一列关联
    # on_delete=models.CASCADE 代表级联删除，当部门中的某个id删除，在员工表中关联的员工也删除
    depart = models.ForeignKey(verbose_name='部门',to='Department',to_field='id',on_delete=models.CASCADE)
    # on_delete=models.CASCADE 代表级联删除，当部门中的某个id删除，在员工表中关联的员工这一项为null
    # depart = models.ForeignKey(to='Department', to_field='id',null=True,blank=True, on_delete=models.SET_NULL)

    #在diango中设置的约束：
    gender_choices = (
        (1,'男'),
        (2,'女'),
    )
    gender = models.SmallIntegerField(verbose_name='性别',choices=gender_choices)

class PrettyNum(models.Model):
    """靓号表"""
    mobile = models.CharField(verbose_name='手机号',max_length=32)
    price = models.IntegerField(verbose_name='价格',default=0)
    level_choices=(
        (1,"一级"),
        (2,"二级"),
        (3,"三级"),
        (4,"四级"),
    )
    level = models.SmallIntegerField(verbose_name="级别",choices=level_choices,default=1)
    status_choices=(
        (1,"未占用"),
        (2,"已占用"),
    )
    status=models.SmallIntegerField(verbose_name="状态",choices=status_choices,default=2)


class FlightTicket(models.Model):
    """飞机票信息"""
    start_end = models.CharField(verbose_name='出发—终点',max_length=1000)
    time_day = models.CharField(verbose_name='日期',max_length=1000)
    airline= models.CharField(verbose_name='航空公司',max_length=1000)
    vecto= models.CharField(verbose_name='飞机型号',max_length=1000)
    start_location= models.CharField(verbose_name='出发机场',max_length=1000)
    start_time= models.CharField(verbose_name='出发时间',max_length=1000)
    end_time= models.CharField(verbose_name='到达时间',max_length=1000)
    end_location= models.CharField(verbose_name='到达机场',max_length=1000)
    floor_price= models.IntegerField(verbose_name='票价')
    url= models.CharField(verbose_name='信息来源',max_length=1000)

class Task(models.Model):
    """任务"""
    level_choice = (
        (1, "紧急"),
        (2, "重要"),
        (3, "一般")
    )
    level = models.SmallIntegerField(verbose_name='级别',choices=level_choice,default=1)
    title = models.CharField(verbose_name='标题',max_length=32)

    user = models.ForeignKey(verbose_name='负责人', to='Admin',on_delete=models.CASCADE)
    detail = models.TextField(verbose_name='详细信息')

class Order(models.Model):
    """订单"""
    oid = models.CharField(verbose_name="订单号", max_length=64)
    title = models.CharField(verbose_name="名称", max_length=32)
    price = models.IntegerField(verbose_name='价格')
    status_choices = (
        (1, "待支付"),
        (2, "已支付"),
    )
    status = models.SmallIntegerField(verbose_name='状态', choices=status_choices, default=1)
    admin = models.ForeignKey(verbose_name='管理员', to='Admin', on_delete=models.CASCADE)

class Boss(models.Model):
    """老板"""
    name = models.CharField(verbose_name='姓名', max_length=32)
    age = models.IntegerField(verbose_name='年龄')
    img = models.CharField(verbose_name='头像', max_length=128)



class City(models.Model):
    """老板"""
    name = models.CharField(verbose_name='名称', max_length=32)
    age = models.IntegerField(verbose_name='人口')
    img = models.FileField(verbose_name='logo', max_length=128,upload_to='city/')
    # 写成FileField，本质上也是CharField，但写成FileField，会自动保存数据，保存到media/city/的文件中

