package com.mintlolly.app.func;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.mintlolly.bean.TableProcess;
import com.mintlolly.common.GmallConfig;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Created on 2022/3/24
 *
 * @author jiangbo
 * Description:
 */
public class TableProcessFunction extends BroadcastProcessFunction<JsonNode, String, JsonNode> {
    OutputTag<JsonNode> hbaseTag = null;
    MapStateDescriptor<String,TableProcess> mapStateDescriptor = null;

    public TableProcessFunction(OutputTag<JsonNode> hbaseTag,MapStateDescriptor<String,TableProcess> mapStateDescriptor) {
        this.hbaseTag = hbaseTag;
        this.mapStateDescriptor = mapStateDescriptor;
    }

    Connection connection = null;

    @Override
    public void open(Configuration parameters) throws Exception {
        //初始化 Phoenix 的连接
        Class.forName(GmallConfig.PHOENIX_DRIVER);
        //定义 Phoenix 的连接
        connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
    }

    @Override
    public void processElement(JsonNode value, ReadOnlyContext ctx, Collector<JsonNode> out) throws Exception {
        //获取状态
        ReadOnlyBroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
        //获取表名和操作类型
        String table = value.get("table").textValue();
        String type = value.get("operation").textValue();
        String key = table + "_" + type;
        if(key.equals("activity_info_create")){
            System.out.println(value);
        }
        //取出对应的配置信息数据
        TableProcess tableProcess = broadcastState.get(key);
        if (tableProcess != null) {
            //向数据中追加 sink_table 信息
            ((ObjectNode) value).put("sink_table", tableProcess.getSinkTable());
            //根据配置信息中提供的字段做数据过滤
            filterColumn(value.get("data"), tableProcess.getSinkColumns());
            //判断当前数据写往哪里
            if (TableProcess.SINK_TYPE_KAFKA.equals(tableProcess.getSinkType())) {
                out.collect(value);
            } else if(TableProcess.SINK_TYPE_HBASE.equals(tableProcess.getSinkType())){
                //hbase 将数据写到侧输出流
                ctx.output(hbaseTag, value);
            }
        }


    }

    private void filterColumn(JsonNode data, String sinkColumns) {
        //保留的数据字段
        String[] fields = sinkColumns.split(",");
        List<String> fieldList = Arrays.asList(fields);
        Iterator<Map.Entry<String, JsonNode>> iterator = data.fields();
        while (iterator.hasNext()) {
            Map.Entry<String, JsonNode> next = iterator.next();
            if (!fieldList.contains(next.getKey())) {
                iterator.remove();
            }
        }
    }

    @Override
    public void processBroadcastElement(String value, Context ctx, Collector<JsonNode> out) throws Exception {
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.setPropertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE);
        JsonNode jsonValue = objectMapper.readTree(value);
        JsonNode data = jsonValue.get("data");
        String table = data.get("source_table").textValue();
        String type = data.get("operate_type").textValue();
        TableProcess tableProcess = objectMapper.readValue(data.toString(), TableProcess.class);
        checkTable(tableProcess.getSinkTable(), tableProcess.getSinkColumns(), tableProcess.getSinkPk(), tableProcess.getSinkExtend());
        //广播出去
        BroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
        String key = table + "_" + type;
        broadcastState.put(key, tableProcess);
    }

    /**
     * Phoenix 建表
     *
     * @param sinkTable   表名 test
     * @param sinkColumns 表名字段 id,name,sex
     * @param sinkPk      表主键 id
     * @param sinkExtend  表扩展字段 ""
     *                    create table if not exists mydb.test(id varchar primary key,name varchar,sex varchar) ...
     */
    private void checkTable(String sinkTable, String sinkColumns, String sinkPk, String sinkExtend) {
        //给主键以及扩展字段附默认值
        if (null == sinkPk) {
            sinkPk = "id";
        }
        if (null == sinkExtend) {
            sinkExtend = "";
        }
        //封装建表sql
        StringBuilder sql = new StringBuilder("create table if not exists ").append(GmallConfig.HBASE_SCHEMA).append(".")
                .append(sinkTable).append("(");
        //遍历添加字段信息
        String[] fields = sinkColumns.split(",");
        for (int i = 0; i < fields.length; i++) {
            String field = fields[i];
            //判断当前字段是否为主键
            if (sinkPk.equals(field)) {
                sql.append(field).append(" varchar primary key");
            } else {
                sql.append(field).append(" varchar");
            }
            if (i < fields.length - 1) {
                sql.append(",");
            }
        }
        sql.append(")");
        sql.append(sinkExtend);
        System.out.println(sql.toString());
        PreparedStatement preparedStatement = null;
        try {
            preparedStatement = connection.prepareStatement(sql.toString());
            preparedStatement.execute();
        } catch (SQLException sqlException) {
            sqlException.printStackTrace();
        } finally {
            try {
                if (preparedStatement != null) preparedStatement.close();
            } catch (SQLException sqlException) {
                sqlException.printStackTrace();
            }
        }

    }
}
