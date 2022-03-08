package com.bingo.func;

import com.alibaba.fastjson.JSONObject;
import com.bingo.bean.DetailBean;
import com.bingo.util.JdbcUtil;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.configuration.Configuration;

import java.lang.reflect.InvocationTargetException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class Filter extends RichFilterFunction<DetailBean> {

    Map<String, Integer> dim;

//    AtomicReference<Map<String, Integer>> mapAtomicReference = new AtomicReference<>();

    @Override
    public void open(Configuration parameters) throws Exception {

//        mapAtomicReference.set(load());
        ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
        scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    load();
                } catch (ClassNotFoundException e) {
                    e.printStackTrace();
                } catch (SQLException e) {
                    e.printStackTrace();
                } catch (InvocationTargetException e) {
                    e.printStackTrace();
                } catch (InstantiationException e) {
                    e.printStackTrace();
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                }
            }
        },5000,5000, TimeUnit.MILLISECONDS);
    }


    //
    public Map<String, Integer> load() throws ClassNotFoundException, SQLException, InvocationTargetException, InstantiationException, IllegalAccessException {
        dim = new HashMap<>();
        JdbcUtil jdbcUtil = new JdbcUtil();
        Class.forName("com.mysql.cj.jdbc.Driver");
        Connection connection = DriverManager.getConnection("jdbc:mysql://172.30.12.108:3306/bgd?serverTimezone=UTC&useUnicode=true&characterEncoding=utf-8&useSSL=false&user=root&password=i@SH021#bg");
        List<JSONObject> queryList = jdbcUtil.queryList(connection, "select * from find_mac", JSONObject.class, false);
        for (JSONObject jsonObject : queryList) {
            System.out.println(jsonObject);
            String mac = jsonObject.getString("mac");
            dim.put(mac,1);
        }
        connection.close();
        return dim;
    }



    @Override
    public boolean filter(DetailBean detailBean) throws Exception {
        try {
            if (dim.containsKey(detailBean.getMac())){
                return true;
            }else {
                return false;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }
}
