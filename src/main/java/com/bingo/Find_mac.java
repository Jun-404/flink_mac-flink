package com.bingo;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.bingo.bean.DetailBean;
import com.bingo.func.CustomerDeserializationSchma;
import com.bingo.func.Filter;
import com.bingo.util.JdbcUtil;
import com.bingo.util.MysqlUtil;
import com.ververica.cdc.connectors.mysql.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.DebeziumSourceFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;

public class Find_mac {

    public static void main(String[] args) throws Exception {


        //1.获取flink执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.setParallelism(10);

//        //开启CK
//        env.enableCheckpointing(5000);
//        env.getCheckpointConfig().setAlignmentTimeout(Duration.ofDays(10000));
//        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.AT_LEAST_ONCE);
//        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
//        //程序重启保存检查点
//        env.getCheckpointConfig().enableExternalizedCheckpoints(
//                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION
//        );
//        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

//        env.setStateBackend(new FsStateBackend("file:///C:/Users/fth/Desktop/flinkCDC"));

        //2.通过flinkCDC构建SourceFunction
        DebeziumSourceFunction<String> sourceFunction = MySqlSource.<String>builder()
                .hostname("172.30.12.101")
                .port(3306)
                .username("root")
                .password("i@SH021.bg")
                .databaseList("bgd")
                .tableList("bgd.data_detail")
                .deserializer(new CustomerDeserializationSchma())
                .startupOptions(StartupOptions.latest())
                .serverTimeZone("Asia/Shanghai")
                .build();

        DataStreamSource<String> dataStreamSource = env.addSource(sourceFunction);

//        转json对象
        SingleOutputStreamOperator<JSONObject> jsonObjDetail = dataStreamSource.map(JSON::parseObject);

        //实体类
        SingleOutputStreamOperator<DetailBean> detailBean = jsonObjDetail.map(jsonObject -> JSONObject.parseObject(jsonObject.getJSONObject("after").toString(), DetailBean.class));

        SingleOutputStreamOperator<DetailBean> filter = detailBean.filter(new Filter());

        filter.addSink(new MysqlUtil());

        filter.print();




        env.execute("find_mac");


    }



}
