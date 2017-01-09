package com.sung.zk.ui.server.zk.op;

import javax.servlet.http.HttpSession;

import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

import com.github.zkclient.ZkClient;

public class ClientCacheManager {
	
	private static final String PRE = "zk-client-";

	public static ZkClient getClient(String cxnString) {
		HttpSession session = ((ServletRequestAttributes) RequestContextHolder.getRequestAttributes()).getRequest().getSession();
		String key = PRE + cxnString;
		ZkClient client = null;
		Object obj = session.getAttribute(key);
		if(obj == null) {
			client = new ZkClient(cxnString, 5000);
			session.setAttribute(key, client);
		} else {
			client = (ZkClient) obj;
		}
		return client;
	}
	
}
