package com.atguigu.realtime.util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * @Author lizhenchao@atguigu.cn
 * @Date 2021/12/27 11:00
 */
public class JdbcUtil {
    public static Connection getJdbcConnection(String driver, String url) throws ClassNotFoundException, SQLException {
        Class.forName(driver);  // 加载驱动
        return DriverManager.getConnection(url);
    }
}
