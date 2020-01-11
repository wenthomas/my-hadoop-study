package com.wenthomas.hive.function;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * @author Verno
 * @create 2020-01-11 14:57
 */

/**
 * 自定义函数：统计传入参数的长度
 */
public class MyUDF extends UDF {
    public int evaluate() {
        return 0;
    }

    public int evaluate(String arg) {
        if (StringUtils.isBlank(arg)) {
            return 0;
        }
        return arg.length();
    }

    public int evaluate(Number arg) {
        if (null == arg) {
            return 0;
        }
        return arg.toString().length();
    }

    public int evaluate(Boolean arg) {
        if (null == arg) {
            return 0;
        }
        return arg.toString().length();
    }
}
