package com.tydic.hbase;

/**
 * @author wanggc
 * @date 2019/08/28 星期三 16:12
 */
import org.apache.hadoop.hbase.client.Connection;

import java.io.*;
import java.util.Locale;
import java.util.Properties;
import java.util.ResourceBundle;

public class test {

/*    public synchronized static Connection getInstance() throws Exception {
        try {
            hConn = null;
            config = null;
            if (hConn == null || hConn.isClosed() || hConn.isAborted()) {
                if (config == null) {
                    config = HBaseConfiguration.create();
                }
                //生产环境
                //config.set("hbase.zookeeper.quorum","bigdata012020,bigdata012011,bigdata012013,bigdata012012,bigdata012021");
                config.set("hbase.zookeeper.quorum",ConfigTools.getInstance("hbaseConfig.yml").getValForStr("hbase.zookeeper.quorum"));
                config.set("kerberos.principal", ConfigTools.getInstance("hbaseConfig.yml").getValForStr("kerberos.principal"));// 这个可以理解成用户名信息，也就是Principal
                config.set("hbase.master.kerberos.principal", ConfigTools.getInstance("hbaseConfig.yml").getValForStr("hbase.master.kerberos.principal"));
                config.set("hbase.regionserver.kerberos.principal", ConfigTools.getInstance("hbaseConfig.yml").getValForStr("hbase.regionserver.kerberos.principal"));
                config.set("hbase.thrift.kerberos.principal", ConfigTools.getInstance("hbaseConfig.yml").getValForStr("hbase.thrift.kerberos.principal"));

                config.set("hbase.zookeeper.property.clientPort", ConfigTools.getInstance("hbaseConfig.yml").getValForStr("hbase.zookeeper.property.clientPort"));

                config.set("hbase.rootdir", ConfigTools.getInstance("hbaseConfig.yml").getValForStr("hbase.rootdir"));
                config.set("zookeeper.znode.parent", ConfigTools.getInstance("hbaseConfig.yml").getValForStr("zookeeper.znode.parent"));
                // 这样我们就不需要交互式输入密码了
                config.set("keytab.file", ConfigTools.getInstance("hbaseConfig.yml").getValForStr("keytab.file"));
                // 这个可以理解成用户名信息，也就是Principal
                //config.set("kerberos.principal", "dic_cuv@MYCDH");
                config.set("hbase.security.authentication", ConfigTools.getInstance("hbaseConfig.yml").getValForStr("hbase.security.authentication"));
                config.set("hbase.rpc.engine", ConfigTools.getInstance("hbaseConfig.yml").getValForStr("hbase.rpc.engine"));
                config.set("hbase.security.authorization", ConfigTools.getInstance("hbaseConfig.yml").getValForStr("hbase.security.authorization"));

                config.set("hadoop.security.authentication", ConfigTools.getInstance("hbaseConfig.yml").getValForStr("hadoop.security.authentication"));
                config.set("hbase.client.retries.number", ConfigTools.getInstance("hbaseConfig.yml").getValForStr("hbase.client.retries.number")); // 重试次数，默认为14，可配置为3
                config.set("zookeeper.recovery.retry", ConfigTools.getInstance("hbaseConfig.yml").getValForStr("zookeeper.recovery.retry"));// zk的重试次数，可调整为3次
                config.set("hbase.client.pause", ConfigTools.getInstance("hbaseConfig.yml").getValForStr("hbase.client.pause")); // 重试的休眠时间，默认为1s，可减少，比如100ms
                config.set("hbase.rpc.timeout", ConfigTools.getInstance("hbaseConfig.yml").getValForStr("hbase.rpc.timeout")); // rpc的超时时间，默认60s，不建议修改，避免影响正常的业务
                config.set("hbase.client.operation.timeout", ConfigTools.getInstance("hbaseConfig.yml").getValForStr("hbase.client.operation.timeout")); // 客户端发起一次数据操作直至得到响应之间总的超时时间,数据操作类型包括get、append、increment、delete、put等
                config.set("hbase.client.scanner.timeout.period", ConfigTools.getInstance("hbaseConfig.yml").getValForStr("hbase.client.scanner.timeout.period")); // scan查询时每次与server交互的超时时间，默认为60s，可不调整。
                config.set("zookeeper.recovery.retry.intervalmill",ConfigTools.getInstance("hbaseConfig.yml").getValForStr("zookeeper.recovery.retry.intervalmill"));// zk重试的休眠时间，默认为1s，可减少，比如：200ms
                config.set("hbase.client.scanner.caching", ConfigTools.getInstance("hbaseConfig.yml").getValForStr("hbase.client.scanner.caching"));
                // 通过keytab登录安全hbase
                UserGroupInformation.setConfiguration(config);
                try {
                    UserGroupInformation.loginUserFromKeytab(ConfigTools.getInstance("hbaseConfig.yml").getValForStr("keytab_name"), ConfigTools.getInstance("hbaseConfig.yml").getValForStr("keytab_locate"));// "hbase/bd-130@MYCDH","/root/hbase.keytab");
                    hConn = ConnectionFactory.createConnection(config);
                    admin = hConn.getAdmin();

                } catch (IOException e) {
                    LOGGER.error("HbaseUtilNew HConnection error ", e);
                    throw e;
                }
            }
        } catch (Exception e) {
            LOGGER.error("HbaseUtilNew getInstance error ", e);
            throw e;
        }
        return hConn;
    }*/


    public static void main(String[] args){
        Locale locale = Locale.ENGLISH;

        if ( args.length != 0 ) {
            locale = new Locale(args[0]);
        }
        ResourceBundle messages = ResourceBundle.getBundle("hb");
        String message = messages.getString("zkQuorum");

        System.out.println(message);

//      Properties  com/tydic/hbase/hb.properties hb.properties
//      E:\IdeaProjects\hbaseTest\src\main\resources\hb.properties
//      E:\IdeaProjects\hbaseTest\src\main\java\hb.properties
/*        Properties prop = new Properties();
        InputStream inputStream = null;
        try {
            //prop.load(test.class.getClassLoader().getResourceAsStream("/hb.properties"));
            inputStream = new BufferedInputStream(new FileInputStream("hb.properties"));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        try {
            prop.load(inputStream);
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println(prop.getProperty("zkQuorum"));
        */

    }
}
