package com.sung.zk.ui.server.zk.web.interceptor;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.sung.zk.ui.server.zk.web.util.AuthUtils;
import org.springframework.web.servlet.handler.HandlerInterceptorAdapter;



public class AuthInterceptor extends HandlerInterceptorAdapter {

	@Override
	public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler) throws Exception {
		if (!AuthUtils.isLogin()) {
			throw new RuntimeException("没有登录");
		}
		return true;
	}
}
