package com.mintlolly.common;

/**
 * Created on 2022/3/24
 *
 * @author jiangbo
 * Description:
 */
public class GmallConfig {
    public static final String HBASE_SCHEMA = "GMALL_REALTIME";
    //Phoenix 驱动
    public static final String PHOENIX_DRIVER = "org.apache.phoenix.jdbc.PhoenixDriver";
    //Phoenix 连接参数
    public static final String PHOENIX_SERVER = "jdbc:phoenix:master,slave1,slave2";
}
