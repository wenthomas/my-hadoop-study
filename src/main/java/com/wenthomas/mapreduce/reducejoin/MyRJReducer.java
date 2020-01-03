package com.wenthomas.mapreduce.reducejoin;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Verno
 * @create 2020-01-03 17:54
 */

/*
 *
 *  order.txt: 1001	01	1
 * 	 pd.txt :  01 小米
 *          orderid,pid,amount,source,pname
 * 1. (null,1001，01，1,order.txt,nodata)
 * (null,nodata，01，nodata,pd.txt,小米)
 *
 * 2. 在输出之前，需要把数据按照source属性分类
 * 		只能在reduce中分类
 */
public class MyRJReducer extends Reducer<NullWritable, MyJBean, NullWritable, MyJBean> {

    private NullWritable k = NullWritable.get();

    //分类的集合
    private List<MyJBean> list = new ArrayList<>();
    private Map<String, Object> map = new HashMap<>();

    @Override
    protected void reduce(NullWritable key, Iterable<MyJBean> values, Context context) throws IOException, InterruptedException {
        //遍历组内所有数据并分装好
        for (MyJBean value : values) {
            //根据文件来源赋值
            if (value.getSource().equals("order.txt")) {
                MyJBean myJBean = new MyJBean();
                try {
                    BeanUtils.copyProperties(myJBean, value);
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                } catch (InvocationTargetException e) {
                    e.printStackTrace();
                }
                list.add(myJBean);
            } else if(value.getSource().equals("pd.txt")) {
                //如果不用Map还用Bean的话依旧是存在reduce循环同一对象的问题
                map.put(value.getPid(), value.getPname());
            } else {
                return;
            }
        }

        //循环集合输出
        //可调用cleanup()方法，reduce()结束前会调用一次
    }

    //循环集合输出结果
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        for (MyJBean myJBean : list) {
            myJBean.setPname(map.get(myJBean.getPid()).toString());
            context.write(k, myJBean);
        }
    }
}
