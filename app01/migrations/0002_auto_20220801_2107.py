from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ('app01', '0001_initial'),
    ]

    operations = [
        migrations.CreateModel(
            name='PrettyNum',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('mobile', models.CharField(max_length=32, verbose_name='手机号')),
                ('price', models.IntegerField(default=0, verbose_name='价格')),
                ('level', models.SmallIntegerField(choices=[(1, '一级'), (2, '二级'), (3, '三级'), (4, '四级')], default=1, verbose_name='级别')),
                ('status', models.SmallIntegerField(choices=[(1, '未占用'), (2, '已占用')], default=2, verbose_name='状态')),
            ],
        ),
        migrations.AlterField(
            model_name='userinfo',
            name='create_time',
            field=models.DateField(verbose_name='入职时间'),
        ),
        migrations.AlterField(
            model_name='userinfo',
            name='depart',
            field=models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to='app01.Department', verbose_name='部门'),
        ),
    ]
