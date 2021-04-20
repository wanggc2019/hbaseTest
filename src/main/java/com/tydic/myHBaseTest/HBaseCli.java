package com.tydic.myHBaseTest;

import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author wanggc
 * @date 2019/09/03 星期二 16:50
 */
public class HBaseCli {

    private static Configuration conf = null;
    //创建一个单例
    private static Connection hconn = null;
    private boolean isConnSuccess = false;
    private static HBaseCli hbaseCli = null;
    //private volatile static HBaseCli hbaseCli = null;


    /** hbase连接zk，带Kerberos
     *
     * @param rootDir
     * @param zkServer zk ip
     * @param zkPort zk port 默认2181
     */
    public HBaseCli(String rootDir,String zkServer,String zkPort){

        conf = HBaseConfiguration.create();
        System.out.println(conf.get("hbase.rootDir"));
        conf.set("hbase.rootdir",rootDir);
        conf.set("hbase.zookeeper.quorum", zkServer);
        conf.set("hbase.zookeeper.property.clientPort",zkPort);
        conf.set("zookeeper.znode.parent", rootDir);
        conf.setInt("hbase.client.retries.number", 3);//客户端重试次数

/*        *//**
         * Kerberos验证
         * *//*
        //String keytab = "E:\\IdeaProjects\\hbaseTest\\eda.keytab";
        //String principal = "eda@MYCDH";
        System.setProperty("java.security.krb5.conf","krb5.conf");//获取Kerberos的kdc等信息
        conf.set("hadoop.security.authentication","Kerberos");
        conf.set("hbase.security.authentication","Kerberos");
        //conf.set("kerberos.principal", "hbase/_HOST@MYCDH");
        conf.set("hbase.master.kerberos.principal", "hbase/_HOST@MYCDH");
        conf.set("hbase.regionserver.kerberos.principal", "hbase/_HOST@MYCDH");

        UserGroupInformation.setConfiguration(conf);
        try {
            //Login with a keytab by calling the UserGroupInformation API
            System.out.println("UserGroupInformation.getCurrentUser():" + UserGroupInformation.getCurrentUser());
            System.out.println("UserGroupInformation.getLoginUser():" + UserGroupInformation.getLoginUser());

            UserGroupInformation.loginUserFromKeytab("eda@MYCDH","eda.keytab");

            System.out.println("UserGroupInformation.getCurrentUser():" + UserGroupInformation.getCurrentUser());
            System.out.println("UserGroupInformation.getLoginUser():" + UserGroupInformation.getLoginUser());

        } catch (IOException e){
            e.printStackTrace();
        }*/

        /**
         * 3、获取连接池
         * */
        try {
            hconn = ConnectionFactory.createConnection(conf);
            isConnSuccess = true;
        } catch (IOException e){
            e.printStackTrace();
            isConnSuccess = false;
        }
    }

    /**
     * 单例模式
     * 懒汉式，双重检查，线程安全；延迟加载；效率较高
     * */
/*
    public static HBaseCli getInstance(String zkServer, String zkPort,String rootDir){
        try {
            hbaseCli = new HBaseCli(rootDir,zkServer, zkPort);
        } catch(Exception e) {
            e.printStackTrace();
        }
        return hbaseCli;
    }
*/

    public static HBaseCli getInstance(String zkServer,String zkPort,String rootDir){
        if (hbaseCli == null){
            synchronized (HBaseCli.class){
                if (hbaseCli == null){
                    hbaseCli = new HBaseCli(rootDir,zkServer,zkPort);
                }
            }
        }
        return hbaseCli;
    }


    /**
     * 关闭连接
     * */
    public void  closeHConn(){
        try {
            hconn.close();
        } catch (IOException e){
            e.printStackTrace();
        }
    }

    /**
     * 根据namespace列出其下的表
     * list_namespace_tables
     * @param nameSpace
     * @return
     */
    public List<Object> getTbaleNamesByNameSpace(String nameSpace){
        final List<Object> obj = new ArrayList<Object>();
        try {
            Admin admin = hconn.getAdmin();
            TableName[] tableName = admin.listTableNamesByNamespace(nameSpace);
            for (TableName name : tableName){
                //System.out.println("tables==> " + name);
                if (name.getNameAsString().contains(":")){
                    obj.add(name.getNameAsString().split(":")[1]);
                } else {
                    obj.add(name.getNameAsString());
                }
            }
        } catch (IOException e){
            e.printStackTrace();
        }
        return obj;
    }

    /**
     * 获取命名空间
     * list_namespace
     * @return
     */
    public List<String> getNameSpace(){
        final List<String> nameSpace = new ArrayList<String>();
        try {
            Admin admin = hconn.getAdmin();
            NamespaceDescriptor[] namespaceDescriptors = admin.listNamespaceDescriptors();
            for (int i=1;i<namespaceDescriptors.length;i++){
                String ns = namespaceDescriptors[i].getName();
                //System.out.println("namespace==>" + ns);
                nameSpace.add(ns);
            }

        } catch (IOException e){
            e.printStackTrace();
        }
        return nameSpace;
    }


    /**
     * 创建hbase表
     * create table 'eda:test','a'
     */
    public void createTable(){
        try {
            Admin admin = hconn.getAdmin();
            TableName tableName = TableName.valueOf("eda:wgctest_api");
            HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName);
            HColumnDescriptor hColumnDescriptor = new HColumnDescriptor("cf1");
            hTableDescriptor.addFamily(hColumnDescriptor);
            //boolean exist = admin.tableExists(tableName);
            if (!admin.tableExists(tableName)){
                admin.createTable(hTableDescriptor);
            }
            System.out.println("\n-->表创建成功==>" +"\n");
        } catch (IOException e){
            e.printStackTrace();
        }

    }

    /**
     * 禁用hbase表
     * disable 'eda:test'
     * @param tableName
     */
    public void disableTable(String tableName){
        try {
            Admin admin = hconn.getAdmin();
            TableName tbl = TableName.valueOf(tableName);
            //boolean exist = admin.tableExists(tbl);
            if (admin.tableExists(tbl) && admin.isTableEnabled(tbl)){
                try{
                    admin.disableTableAsync(tbl);
                    System.out.println("\n-->表" + tableName +"禁用成功!");
                } catch (IOException e){
                    e.printStackTrace();
                }
            } else {
                System.out.println("\n-->表" + tableName +"不存在!");
            }
        } catch (IOException e){
            e.printStackTrace();
        }
    }

    /**
     * 删除hbase表
     * 需要先disable表，才能删除
     * disable 'eda:test'
     * drop 'eda:test'
     * @param tableName
     */
    public void dropTable(String tableName){
        try {
            Admin admin = hconn.getAdmin();
            TableName tbl = TableName.valueOf(tableName);
            //boolean exixt = admin.tableExists(tbl);
            if (admin.tableExists(tbl)){
                admin.deleteTable(tbl);
                System.out.println("\n-->表"+tableName + "删除成功!\n");
            } else {
                System.out.println("\n-->待删除的表" + tableName+ "不存在!\n");
            }
        } catch (IOException e){
            e.printStackTrace();
            System.out.println("\n-->表" + tableName + "删除失败!\n");
        }
    }

    /**
     * 扫描全表
     * scan 'eda:test'
     * @param tableName
     */
    public void scanTable(String tableName){
        try {
            // 创建tablename对象
            TableName tbl = TableName.valueOf(tableName);
            // 获取连接
            Table table = hconn.getTable(tbl);
            // 创建scan对象
            Scan scan = new Scan();
            //Returns a scanner on the current table as specified by the Scan object.
            ResultScanner resultScanner = table.getScanner(scan);
            System.out.println("\n----扫描开始:["+ tbl + "]----");
            for (Result result:resultScanner){
                System.out.println("scan result: " + result.toString());
                //System.out.println(result);
                byte[] row = result.getRow();
                System.out.println("rowName:" + new String(row,"utf-8"));
                List<Cell> cells = result.listCells();
                System.out.println("scan cells: " + cells);
            }
        } catch (IOException e){
            e.printStackTrace();
        }
        System.out.println("----扫描结束----\n");
    }

    /**
     * 插入数据
     * put 'eda:test','row1','f1:name','张三丰'
     * @param tableName
     */
    public void putTable(String tableName){
        try{
            Table table = hconn.getTable(TableName.valueOf(tableName));
            //定义行
            byte[] row = Bytes.toBytes("row1");
            //要插入的列族
            byte[] columnFamily  = Bytes.toBytes("f1");
            //列族修饰词
            byte[] qualifier = Bytes.toBytes("name");
            //值
            byte[] value = Bytes.toBytes("张三丰");
            //创建put对象
            Put put = new Put(row);
            //将待插入的数据添加到put对象
            put.addColumn(columnFamily,qualifier,value);
            //向表插入数据
            table.put(put);
        } catch (IOException e){
            e.printStackTrace();
        }
    }

    /**
     * 获取数据
     * get 'eda:test','row1'
     * @param tableName
     */
    public void getTable(String tableName){
        try {
            Table table = hconn.getTable(TableName.valueOf(tableName));
            byte[] row = Bytes.toBytes("row1");
            //定义get对象
            Get get = new Get(row);
            //通过table对象获取数据
            Result result = table.get(get);
            System.out.println();
            System.out.println("-->get result: " + result);
            //只获取value
            byte[] valueByte = result.getValue(Bytes.toBytes("f1"),Bytes.toBytes("name"));
            System.out.println("-->get valueByte: " + valueByte);
            //转化为字符串
            String valueString = new String(valueByte);
            System.out.println("-->get valueString: " + valueString);
        } catch (IOException e){
            e.printStackTrace();
        }
        System.out.println();
    }

    /**
     * 主函数 main
     * @param args
     */
    public static void main(String[] args) {
        // 输入要连接的集群的zk信息
        HBaseCli cli = HBaseCli.getInstance("bigdata014231,bigdata014232,bigdata014233","2181","/hbase");

        //根据namespace列出其下的表
        //List<Object> obj = cli.getTbaleNamesByNameSpace("eda");
        //System.out.println("\n-->获取命名空间eda下的所有表 ==>\n" + obj + "\n");

        //获取命名空间
        //List<String> namespace = cli.getNameSpace();
        //System.out.println("\n-->获取hbase所有的命名空间 ==>\n" + namespace + "\n");

        //创建表
        //cli.createTable();

        //删除表,要先disable表
        //cli.disableTable("eda:test998");
        //cli.dropTable("eda:test998");

        //scan 表
        //cli.scanTable("eda:test");

        //查入数据
        //cli.putTable("eda:test");
        //cli.scanTable("eda:test");

        //获取数据
        cli.getTable("eda:test");

        //关闭连接
        cli.closeHConn();

    }

    public boolean isConnSuccess(){
        return isConnSuccess;
    }

    public void setConnSuccess(boolean isConnSuccess){
        this.isConnSuccess = isConnSuccess;
    }

    public Connection getHconn(){
        return hconn;
    }

    public void setHconn(Connection hconn){
        HBaseCli.hconn = hconn;
    }

    public Configuration getConf(){
        return conf;
    }

    public void setConf(Configuration conf){
        HBaseCli.conf = conf;
    }

}
