package com.sung.zk.ui.server.zookeeper.init;

import com.sung.zk.ui.server.zookeeper.config.LoadConfiguration;
import com.sung.zk.ui.server.zookeeper.zk.ZkHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.Servlet;
import javax.servlet.http.HttpServlet;

public class Initialization extends HttpServlet implements Servlet {

    private static final long serialVersionUID = 4191303794602069576L;

    private Logger LogUtils = LoggerFactory.getLogger(Initialization.class);

    public void init() {

        initSystem();

    }

    private void initSystem() {
        LogUtils.info("=================================Start to init system===========================");
        // 加载一些配置文件
        new LoadConfiguration();
        //初始化ZK 链接
        new ZkHelper();

        // 监听 ZK 配置
        try {
            //ZkConfigParamListener.loadValue();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
