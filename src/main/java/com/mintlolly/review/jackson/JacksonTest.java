package com.mintlolly.review.jackson;

import com.fasterxml.jackson.core.*;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.*;
import java.util.Iterator;
import java.util.Map;

/**
 * Created on 2022/3/10
 *
 * @author jiangbo
 * Description:
 */
public class JacksonTest {
    public static void main(String[] args) {
        ObjectMapper obm = new ObjectMapper();
        Student student = new Student("小王",22);
        try {
            String asString = obm.writeValueAsString(student);
            System.out.println(asString);
            Iterator<Map.Entry<String, JsonNode>> fields = obm.readTree(asString).fields();
            while (fields.hasNext()){
                System.out.println(fields.next().getKey());
            }

            Student obmStudent = obm.readValue(asString, Student.class);
            System.out.println(obmStudent.toString());
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }

        //使用JsonGenerator写入JSON
        JsonFactory jsonFactory = new ObjectMapper().getFactory();
        StringWriter sw = new StringWriter();
        try {
            JsonGenerator generator = jsonFactory.createGenerator(sw);
            generator.writeStartObject();
            generator.writeStringField("name","小王");
            generator.writeNumberField("age",22);
            generator.writeObjectField("student",student);
            generator.writeEndObject();
            generator.flush();
            generator.close();
            System.out.println(sw);
        } catch (IOException e) {
            e.printStackTrace();
        }
//        使用JsonParser 读取JSON
        JsonParser jsonParser = null;
        try {
            jsonParser = jsonFactory.createParser(new File("student.json"));
            while (jsonParser.nextToken() != JsonToken.END_OBJECT) {
                String fieldname = jsonParser.getCurrentName();
                if ("name".equals(fieldname)) {
                    //move to next token
                    jsonParser.nextToken();
                    System.out.println(jsonParser.getText());
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        String s = "{\"operate_type\":\"insert\",\"sink_type\":\"hbase\",\"sink_table\":\"dim_activity_info\",\"source_table\":\"activity_info\",\"sink_extend\":null,\"sink_pk\":\"id\",\"sink_columns\":\"id,activity_name,activity_type,activity_desc,start_time,end_time,create_time\"}";
    }
}
