package com.mintlolly.app.func;

import com.fasterxml.jackson.databind.JsonNode;
import com.mintlolly.common.GmallConfig;
import com.mysql.cj.jdbc.ClientPreparedStatement;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.IterationRuntimeContext;
import org.apache.flink.api.common.functions.RichFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.*;

/**
 * Created on 2022/3/28
 *
 * @author jiangbo
 * Description:
 */
public class DimSink extends RichSinkFunction<JsonNode> {
    Connection conn = null;

    @Override
    public void open(Configuration parameters) throws Exception {
        //初始化hbase连接
        Class.forName(GmallConfig.PHOENIX_DRIVER);
        conn = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
    }

    @Override
    public void invoke(JsonNode value, Context context) throws Exception {
        PreparedStatement preparedStatement = null;
        JsonNode data = value.get("data");
        Iterator<Map.Entry<String, JsonNode>> fields = data.fields();
        Set<String> keys = new HashSet<>();
        Collection<Object> values = new ArrayList<>();
        while (fields.hasNext()) {
            Map.Entry<String, JsonNode> next = fields.next();
            keys.add(next.getKey());
            values.add(next.getValue());
        }
        String sink_table = value.get("sink_table").textValue();
        String upsertSql = genUpsertSql(sink_table, keys, values);
        System.out.println(upsertSql);
        //编译 SQL
        preparedStatement = conn.prepareStatement(upsertSql);
        //执行
        preparedStatement.executeUpdate();
        //提交
        conn.commit();
    }

    //创建插入数据的 SQL upsert into t(id,name,sex) values('...','...','...')
    private String genUpsertSql(String tableName, Set<String> keys, Collection<Object> values) {
        return "upsert into " + GmallConfig.HBASE_SCHEMA + "." +
                tableName + "(" + StringUtils.join(keys, ",") + ")" +
                " values('" + StringUtils.join(values, "','") + "')";
    }
}
