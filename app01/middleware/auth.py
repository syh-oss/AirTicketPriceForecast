from django.utils.deprecation import MiddlewareMixin
from django.shortcuts import render,HttpResponse,redirect


class AuthMiddleware(MiddlewareMixin):
    """中间件"""

    def process_request(self,request):
        # 排除那些不需要登录就能访问的页面
        if request.path_info in['/login/',"/image/code/",'/order/add/','/admin/add/']:
            return
        # 需要登录才能访问的页面，就要读取当前用户的session信息，如果能读到，说明已登录，可以继续
        info_dict = request.session.get("info")
        if info_dict:
            return

        # 剩下的是没有登录，重新回到登录页面
        return redirect('/login/')

    # def process_response(self,request,response):
    #     print("M1.走了")
    #     return response



