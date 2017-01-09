package com.sung.zk.ui.server.zk.web.freemarker;

import java.util.List;


import com.sung.zk.ui.server.zk.web.util.AuthUtils;
import freemarker.template.TemplateMethodModelEx;
import freemarker.template.TemplateModelException;

public class IsLogin implements TemplateMethodModelEx {

	@SuppressWarnings("rawtypes")
	public Object exec(List arg0) throws TemplateModelException {
		return AuthUtils.isLogin();
	}

}
