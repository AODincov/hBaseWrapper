package ru.mmtr.at.hbase.connection;

import lombok.SneakyThrows;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;

public class ConnectionFactory {

    @SneakyThrows
    public static Connection getConnection() {
        Configuration conf = HBaseConfiguration.create();
        conf.clear();
        //todo вынести в .properties
        conf.set("hbase.zookeeper.quorum", "hbase-docker");
        conf.set("hbase.zookeeper.property.clientPort","2181");
        conf.set("zookeeper.znode.parent", "/hbase");
        conf.set("hbase.master", "127.0.0.1:16000");

        conf.set("hbase.client.retries.number", "3");  // default 35
        conf.set("hbase.rpc.timeout", "10000");  // default 60 secs
        conf.set("hbase.rpc.shortoperation.timeout", "5000"); // default 10 secs

        return org.apache.hadoop.hbase.client.ConnectionFactory.createConnection(conf);
    }
}
