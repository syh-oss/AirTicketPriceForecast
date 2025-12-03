from django import forms

class BootStrapModelForm(forms.ModelForm):
    #设置初始化函数，是为了从fields中提取其中的每个元素，赋予插件，并在插件属性中设置class的值
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        for name, field in self.fields.items():
            #字段中有属性，保留原来的属性并增加新属性，没有属性，才设置
            if field.widget.attrs:    # 如果字段中widget有值；
                field.widget.attrs['class'] = "form-control"
                field.widget.attrs['placeholder'] = field.label
            else:
                field.widget.attrs = {"class":"form-control",'placeholder':field.label}

class BootStrapForm(forms.Form):
    #设置初始化函数，是为了从fields中提取其中的每个元素，赋予插件，并在插件属性中设置class的值
    bootstrap_exclude_fields=[]
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        for name, field in self.fields.items():
            if name in self.bootstrap_exclude_fields:
                continue
            #字段中有属性，保留原来的属性并增加新属性，没有属性，才设置
            if field.widget.attrs:    # 如果字段中widget有值；
                field.widget.attrs['class'] = "form-control"
                field.widget.attrs['placeholder'] = field.label
            else:
                field.widget.attrs = {"class":"form-control",'placeholder':field.label}