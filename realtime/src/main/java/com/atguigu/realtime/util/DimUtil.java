package com.atguigu.realtime.util;

import com.alibaba.fastjson.JSONObject;

import java.sql.Connection;
import java.util.List;

public class DimUtil {
    public static JSONObject readDimFromPhoenix(Connection phoenixConn, String table, Long id) {

        String sql = "select * from " + table + " where id=?";

        Object[] args={id.toString()};
        List<JSONObject> list = JdbcUtil.queryList(phoenixConn, sql,args, JSONObject.class);

        return list.size()==0?new JSONObject():list.get(0);
    }
}
