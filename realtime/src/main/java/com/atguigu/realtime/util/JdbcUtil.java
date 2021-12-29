package com.atguigu.realtime.util;

import com.alibaba.fastjson.JSONObject;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;


/**
 * @author yj2333
 */
public class JdbcUtil {
    public static Connection getJdbcConnection(String driver, String url) throws ClassNotFoundException, SQLException {
        Class.forName(driver);  // 加载驱动
        return DriverManager.getConnection(url);
    }

    //参数依次是 数据库连接器     sql语句   反射  TODO
    public static <T> List<T> queryList(Connection conn,
                                        String sql,
                                        Object[] args ,
                                        Class<T> tClass){
        System.out.println("test");
        return null;
    }
}
