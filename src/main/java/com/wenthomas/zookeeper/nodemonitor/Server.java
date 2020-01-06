package com.wenthomas.zookeeper.nodemonitor;

import org.apache.zookeeper.*;

import java.io.*;
import java.util.Properties;

/**
 * @author Verno
 * @create 2020-01-04 17:13
 */

/*
 * 1.每次启动后，在执行自己的核心业务之前，先向zk集群注册一个临时节点
 * 		且向临时节点中保存一些关键信息
 */
public class Server {
    //服务集群
    private static String connectString = "hadoop01:2181,hadoop02:2181,hadoop03:2181";

    //该超时时间根据配置文件需要在4000~40000之间，超出范围默认取两边
    private int sessionTimeout = 6000;

    private ZooKeeper zooKeeper;

    //服务根节点目录
    private String basePath="/Servers";

    //读取服务配置文件
    public String loadConfig() throws IOException {
        Properties properties = new Properties();
        //通过反射方法获取resource目录下的配置文件
        InputStream is = Server.class.getClassLoader().getResourceAsStream("zk-server-node.properties");
        properties.load(is);
        String node = properties.getProperty("server2");
        return node;
    }


    public static void main(String[] args) throws IOException, KeeperException, InterruptedException {

        Server server = new Server();
        //初始化ZK客户端对象
        server.init();
        //向服务集群注册临时节点
        String info = server.loadConfig();
        server.regist(info);
        //执行当前服务的其他业务
        server.business();
    }

    /**
     * 初始化ZK客户端对象
     */
    public void init() throws IOException {
        zooKeeper = new ZooKeeper(connectString, sessionTimeout, new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {

            }
        });
    }


    /**
     * 使用zk客户端注册临时节点
     * @param info : 服务基本信息
     */
    private void regist(String info) throws KeeperException, InterruptedException {
        //节点必须是临时待序号的节点
        zooKeeper.create(basePath + "/server", info.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
    }

    /**
     * 当前服务的其他业务功能
     */
    public void business() throws InterruptedException {
        System.out.println("working......");

        //持续工作
        while(true) {

            Thread.sleep(5000);

            System.out.println("working......");

        }
    }
}
