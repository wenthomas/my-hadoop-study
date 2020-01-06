package com.wenthomas.zookeeper.test;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

/**
 * @author Verno
 * @create 2020-01-04 16:55
 */
public class MyZKClient {
    //服务集群
    private static String connectString = "hadoop01:2181,hadoop02:2181,hadoop03:2181";

    //该超时时间根据配置文件需要在4000~40000之间，超出范围默认取两边
    private int sessionTimeout = 6000;

    private ZooKeeper zooKeeper;

    //初始化创建客户端
    @Before
    public void init() throws IOException {
        //获取ZK客户端
        zooKeeper = new ZooKeeper(connectString, sessionTimeout, new Watcher() {
            //回调方法：收到事件通知后的回调函数（用户的业务逻辑）
            //回调方法，一旦watcher观察的path触发了指定的事件，服务端会通知客户端，客户端收到通知后
            // 会自动调用process()
            @Override
            public void process(WatchedEvent watchedEvent) {

            }
        });
        System.out.println(zooKeeper);
    }

    @After
    public void close() throws InterruptedException {
        if (null != zooKeeper) {
            zooKeeper.close();
        }
    }


    // ls
    @Test
    public void ls() throws Exception {

        Stat stat = new Stat();

        List<String> children = zooKeeper.getChildren("/", null, stat);

        System.out.println(children);

        System.out.println(stat);

    }

    // create [-s] [-e] path data
    @Test
    public void create() throws Exception {

        zooKeeper.create("/idea", "hello".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

    }

    // get path
    @Test
    public void get() throws Exception {

        byte[] data = zooKeeper.getData("/eclipse", null, null);

        System.out.println(new String(data));


    }

    // set path data
    @Test
    public void set() throws Exception {

        zooKeeper.setData("/eclipse", "hi".getBytes(), -1);

    }

    // delete path
    @Test
    public void delete() throws Exception {

        zooKeeper.delete("/eclipse", -1);

    }

    // 递归删除节点：rmr path
    @Test
    public void rmr() throws Exception {

        String path="/data";

        //先获取当前路径中所有的子node
        List<String> children = zooKeeper.getChildren(path, false);

        //删除所有的子节点
        for (String child : children) {

            zooKeeper.delete(path+"/"+child, -1);

        }

        zooKeeper.delete(path, -1);

    }

    // 判断当前节点是否存在
    @Test
    public void ifNodeExists() throws Exception {

        Stat stat = zooKeeper.exists("/data2", false);

        System.out.println(stat==null ? "不存在" : "存在");

    }
}
