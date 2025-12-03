"""
自定义分页组件,需要做如下几件事：

在视图函数中：
    def pnum_list(request):

        1、根据自己的情况去筛选自己的数据
        queryset = models.PrettyNum.objects.all()

        2、实例化分页对象
        page_object = Pagination(request,queryset)
        context={
            'queryset':page_object.page_queryset,   #分完页的数据
            'page_string':page_object.html()        #生成页码
                 }
        return render(request,'pnum_list.html',context)

在HTML页面中

    {% for obj in queryset %}
        {obj.xx}
    {% endfor %}

    <ul class="pagination" >
        {{  page_string }}
    </ul>

"""
from django.utils.safestring import mark_safe
class Pagination(object):
    def __init__(self,request,queryset,page_size = 10,page_param="page",plus = 5):
        """

        :param request: 请求的对象（不改）
        :param queryset: 符合条件的数据（根据这个数据进行分页处理）
        :param page_size: 每页显示多少条数据
        :param page_param: 在url中传递的获取分页的参数，例如：/pnum_list/?page=12
        :param plus:  显示当前页的前几页或后几页（页码）
        """
        from django.http.request import QueryDict
        import copy
        query_dict = copy.deepcopy(request.GET)
        query_dict._mutable = True
        self.query_dict = query_dict


        self.page_param = page_param
        page = request.GET.get(page_param,'1')
        if page.isdecimal():   # 进行验证，防止输入的不是数字
            page=int(page)

        else:
            page=1
        self.page=page
        self.page_size=page_size
        self.start = (page - 1) * page_size
        self.end = page * page_size
        self.page_queryset = queryset[self.start:self.end]

        total_count = queryset.count()
        total_page_count, div = divmod(total_count, page_size)
        if div:
            total_page_count += 1
        self.total_page_count = total_page_count
        self.plus=plus

    def html(self):
        # 计算出，显示当前页的前5页，后5页

        if self.total_page_count <= 2 * self.plus + 1:
            # 数据库中的数据没有达到11页
            start_page = 1
            end_page = self.total_page_count+1
        else:
            # 数据库中的数据多于11页
            # 当前页小于5页
            if self.page <= self.plus:
                start_page = 1
                end_page = 2 * self.plus + 1
            else:
                # 当前页 > 5
                #  当前页+5> 总页码
                if (self.page + self.plus) > self.total_page_count:
                    start_page = self.total_page_count - 2 * self.plus
                    end_page = self.total_page_count + 1
                else:
                    start_page = self.page - self.plus
                    end_page = self.page + self.plus + 1

        # 页码
        page_str_list = []



        # 首页
        self.query_dict.setlist(self.page_param, [1])
        page_str_list.append(f'<li><a href="?{self.query_dict.urlencode()}">首页 </a></li>')
        # 上一页
        if self.page > 1:
            self.query_dict.setlist(self.page_param, [self.page - 1])
            prev = f'<li><a href="?{self.query_dict.urlencode()}">上一页 </a></li>'
        else:
            self.query_dict.setlist(self.page_param, [1])
            prev = f'<li><a href="?{self.query_dict.urlencode()}">上一页 </a></li>'
        page_str_list.append(prev)
        for i in range(start_page, end_page):
            self.query_dict.setlist(self.page_param, [i])
            if i == self.page:
                ele = f'<li class="active"><a  href="/?{self.query_dict.urlencode()}">{i} </a></li>'
            else:
                ele = f'<li><a href="?{self.query_dict.urlencode()}">{i} </a></li>'
            page_str_list.append(ele)

        # 下一页
        if self.page < self.total_page_count:
            self.query_dict.setlist(self.page_param, [self.page + 1])
            after = f'<li><a href="?{self.query_dict.urlencode()}">下一页 </a></li>'
        else:
            self.query_dict.setlist(self.page_param, [self.total_page_count])
            after = f'<li><a href="?{self.query_dict.urlencode()}">下一页 </a></li>'
        page_str_list.append(after)
        # 尾页
        self.query_dict.setlist(self.page_param, [self.total_page_count])
        page_str_list.append(f'<li><a href="?{self.query_dict.urlencode()}">尾页 </a></li>')
        # 分页中的搜索模块
        search_string = """
              <form method="get" style="float:left; margin-left:2px;">
                          <div class="input-group" style="width:200px">
                              <input type="text" name="page" class="form-control" placeholder="页码">
                              <span class="input-group-btn">
                                  <button class="btn btn-default" type="submit">跳转</button>
                              </span>

                          </div>    
            """
        page_str_list.append(search_string)
        page_string = mark_safe("".join(page_str_list))
        return page_string