package com.bingo.func;

import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.List;

public class CustomerDeserializationSchma implements DebeziumDeserializationSchema<String> {

    @Override
    public void deserialize(SourceRecord sourceRecord, Collector<String> collector) throws Exception {

        JSONObject result = new JSONObject();

        String topic = sourceRecord.topic();
        String[] arr = topic.split("\\.");
        String db = arr[1];
        String tableName = arr[2];

        //获取值信息并转换为 Struct 类型
        Struct value = (Struct) sourceRecord.value();
        //获取变化后的数据
        Struct after = value.getStruct("after");
        JSONObject afterJson = new JSONObject();
        Schema schema = after.schema();
        List<Field> fieldList = schema.fields();
        for (Field field : fieldList) {
            afterJson.put(field.name(),after.get(field));
        }
        result.put("after",afterJson);

//        //获取操作类型
//        Envelope.Operation operation = Envelope.operationFor(sourceRecord);
//        result.put("op",operation);

        //输出数据
        collector.collect(result.toJSONString());

    }

    @Override
    public TypeInformation<String> getProducedType() {
        return BasicTypeInfo.STRING_TYPE_INFO;
    }

}
