package com.bingo.util;


import com.bingo.bean.DetailBean;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class MysqlUtil extends RichSinkFunction<DetailBean> {
    private Connection connection;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        Class.forName("com.mysql.cj.jdbc.Driver");
        connection = DriverManager.getConnection("jdbc:mysql://172.30.12.108:3306/bgd?serverTimezone=UTC&useUnicode=true&characterEncoding=utf-8&useSSL=false&user=root&password=i@SH021#bg");

    }

    @Override
    public void invoke(DetailBean value, Context context) throws Exception {
        PreparedStatement preparedStatement = null;
        try {
            //获取SQL语句
            String insertSql = getUpsertSql("find_detail", value);

            System.out.println(insertSql);

            preparedStatement = connection.prepareStatement(insertSql);
            preparedStatement.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            if (preparedStatement != null){
                preparedStatement.close();
            }
        }

    }

    private String getUpsertSql(String sinkTable,DetailBean detailBean){
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        return "insert into "+ sinkTable+"(dataid,ranges,rssi0,dtime,in_time,mmac,mac,essid0) values ('"
                +detailBean.getDataid()+"','"
                +detailBean.getRanges()+"','"
                +detailBean.getRssi0()+"','"
                +simpleDateFormat.format(new Date(detailBean.getDtime()-8*60*60*1000))+"','"
                +simpleDateFormat.format(new Date(detailBean.getIn_time()-8*60*60*1000))+"','"
                +detailBean.getMmac()+"','"
                +detailBean.getMac()+"',NULL)";
    }

}
