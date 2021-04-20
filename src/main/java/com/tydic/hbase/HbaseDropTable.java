package com.tydic.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;

//author:hegq
public class HbaseDropTable {

    public static String getTableName = null ;

    //创建一个单例
    public static Connection connection = null ;

    //============配置参数===========================================================
    public static boolean printLog ;
    public static String zkQuorum ;
    public static String keytab ;
    public static String principal ;
    public static int sleepSec ;

    public HbaseDropTable() {
    }

    //从配置文件加载
    static {
        ResourceBundle resourceBundle = ResourceBundle.getBundle("hb");

        printLog = resourceBundle.getString("printLog").equals("1") ? true:false ;
        zkQuorum = resourceBundle.getString("zkQuorum");
        keytab = resourceBundle.getString("keytab");
        principal = resourceBundle.getString("principal");
        sleepSec = Integer.parseInt(resourceBundle.getString("sleepSec")!=null ? resourceBundle.getString("sleepSec"): "1");

        System.out.println("zkQuorum:[" + zkQuorum + "]" );
        System.out.println("keytab:[" + keytab + "], principal=[" + principal + "],sleepSec=["  + sleepSec + "]");
    }

    //=============================================================================================
    public static void main(String[] args) throws IOException, Exception {
/*
        if(args.length != 1) {
            System.out.println("usage HbaseDropTable TableName");
            System.exit(2);
        }
*/

        getTableName = "eda:hive_create_hbase";

        System.out.println("getTableName:[" + getTableName + "]" );

        //初始化
        if(init()<0)
            System.exit(2);

        //查询
        doDrop();

        //退出
        destroy();
    }

    //销毁连接
    public static void destroy() {

        if(connection!=null)
            try {
                connection.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
    }

    //初始化连接
    public static int init()  {

        System.out.println("-----enter  init ----- ");

        //以下保证只在初始化入口执行一次
        Configuration conf = HBaseConfiguration.create();
        System.setProperty("java.security.krb5.conf", "krb5.conf");
        conf.set("hbase.zookeeper.quorum",zkQuorum );
        conf.set("hadoop.security.authentication", "kerberos");
        conf.set("hbase.security.authentication", "kerberos");
        conf.set("kerberos.principal", "hbase/_HOST@MYCDH");
        conf.set("hbase.master.kerberos.principal", "hbase/_HOST@MYCDH");
        conf.set("hbase.regionserver.kerberos.principal", "hbase/_HOST@MYCDH");
        conf.set("hbase.client.retries.number", "1");

        // 通过keytab登录安全kerberos
        UserGroupInformation.setConfiguration(conf) ;
        try {
            UserGroupInformation.loginUserFromKeytab(principal,keytab);
        } catch (IOException e) {
            e.printStackTrace();
            return -1;
        }
        System.out.println("hbase login sucess!");

        //实例化出来connection单例
        try {
            connection = ConnectionFactory.createConnection(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }


        return 0 ;
    }

    //开始删除
    public static void doDrop() throws Exception, IOException {


        TableName tableName = TableName.valueOf(getTableName);
        HBaseAdmin admin = (HBaseAdmin) connection.getAdmin();
        if (admin.tableExists(tableName)) {
            System.out.println("待删除表 【"+getTableName+"】 存在");
            try {
                admin.disableTable(tableName);
                admin.deleteTable(tableName);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }else{
            System.out.println("待删除表 【"+getTableName+"】 不存在");
        }

        System.out.println("**hbase drop table finished !**");
    }
}

