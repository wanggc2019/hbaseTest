package com.tydic.test;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.security.UserGroupInformation;

public class HbaseCli {

    private boolean isConnectSuccess = false;
    private static HConnection conn;
    private static Configuration conf = null;
    public static HbaseCli HbaseCli = null;
    HTable table = null; //添加，与查询对应
    public HbaseCli(String rootDir,String zkServer,String zkPort) {

        System.setProperty("java.security.krb5.conf","krb5.conf");//获取Kerberos的kdc等信息
        conf = HBaseConfiguration.create();
        System.out.println(conf.get("hbase.rootdir"));
        conf.set("hbase.rootdir", rootDir);
        conf.set("hbase.zookeeper.quorum", zkServer);
        conf.set("hbase.zookeeper.property.clientPort",zkPort);
        conf.set("zookeeper.znode.parent", rootDir);

        conf.setInt("hbase.client.retries.number", 1);

        String keytab = "eda.keytab";
        String principal ="eda@MYCDH";
        //String dynamicPrincipal = "hbase/_HOST@MYCDH";

        //Configuration conf = new Configuration();
        conf.set("hadoop.security.authentication", "Kerberos");
        conf.set("hbase.security.authentication","kerberos");

        //conf.set("hbase.master.keytab.file", keytab);
        //conf.set("hbase.master.kerberos.principal", principal);

        //conf.set("hbase.regionserver.keytab.file", keytab);
        //conf.set("hbase.regionserver.kerberos.principal", principal);

        //conf.set("hbase.rpc.engine", "org.apache.hadoop.hbase.ipc.SecureRpcEngine");

        //conf.set("kerberos.principal", "hbase/_HOST@MYCDH" );
        conf.set("hbase.master.kerberos.principal","hbase/_HOST@MYCDH");
        //conf.set("hbase.regionserver.kerberos.principal","hbase/_HOST@MYCDH");

        //conf.set("hbase.regionserver.kerberos.principal", dynamicPrincipal);

        UserGroupInformation.setConfiguration(conf);
        try {
            System.out.println("UserGroupInformation.getCurrentUser():"
                    + UserGroupInformation.getCurrentUser());

            System.out.println("UserGroupInformation.getLoginUser():"
                    + UserGroupInformation.getLoginUser());

            UserGroupInformation.loginUserFromKeytab(principal, keytab);

            System.out.println("UserGroupInformation.getCurrentUser():"
                    + UserGroupInformation.getCurrentUser());
            System.out.println("UserGroupInformation.getLoginUser():"
                    + UserGroupInformation.getLoginUser());
        } catch (IOException e1) {
            e1.printStackTrace();
        }

        try {
            conn = HConnectionManager.createConnection(conf);
            isConnectSuccess = true;

        }catch (IOException e) {
            e.printStackTrace();
            isConnectSuccess = false;

        }
    }

    //单例模式
    public static HbaseCli getInstance(String zkServer,
                                       String zkPort,String rootDir){

        try {

            HbaseCli = new HbaseCli(rootDir,zkServer, zkPort);

        } catch(Exception e) {
            e.printStackTrace();
        }
        return HbaseCli;
    }

    public void close(){
        try {
            conn.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    /**
     * get hbase table list by name space
     * @return
     * @throws IOException
     */
    public List<Object> getTableNamesByNamespace(final String nameSpace) {

        final List<Object> objects = new ArrayList<Object>();
        try {
            HBaseAdmin admin = new HBaseAdmin(conf);

            TableName[] tables = admin.listTableNamesByNamespace(nameSpace);
            for(TableName name :tables){
                System.out.println("--->tables ="+name);
                if(name.getNameAsString().contains(":")){
                    objects.add(name.getNameAsString().split(":")[1]);
                } else {
                    objects.add(name.getNameAsString());
                }

            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return objects;
    }

    /**
     * 获取命名空间
     */
    public List<String> getNameSpace(){
        //login();
        final List<String> nameSpaces = new ArrayList<String>();
        try {
            HBaseAdmin admin = new HBaseAdmin(conf);
            String[] a = admin.getTableNames();
            System.out.println( admin.getTableNames());
            NamespaceDescriptor[] nameSpaceArray=admin.listNamespaceDescriptors();

            for (int i = 0; i < nameSpaceArray.length; i++){
                String nameSpace = nameSpaceArray[i].getName();
                System.out.println("--->nameSpace ="+nameSpace);
                nameSpaces.add(nameSpace);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }

        return nameSpaces;

    }

    //创建表
    public void createTable(){
        User user = User.create(UserGroupInformation.createRemoteUser("hbase"));
        //login();
        try {
            user.runAs(new PrivilegedExceptionAction<Object>(){
                public Object run() throws Exception {
                    try {
                        HBaseAdmin hBaseAdmin = new HBaseAdmin(conf);
                        // 设置表名   这个类是表名和列族的容器
                        HTableDescriptor hTableDescriptor = new HTableDescriptor("qxm_teacher4");
                        // 设置两个列族名
                        HColumnDescriptor basecolumnDescriptor = new HColumnDescriptor("base");
                        HColumnDescriptor morecolumnDescriptor = new HColumnDescriptor("more");
                        // 列族加入到表中
                        hTableDescriptor.addFamily(basecolumnDescriptor);
                        hTableDescriptor.addFamily(morecolumnDescriptor);
                        // 创建之
                        boolean exist = hBaseAdmin.tableExists("teacher4");
                        if(!exist){
                            hBaseAdmin.createTable(hTableDescriptor);
                        }



                        System.out.println("创建的新表teacher成功了吗? " + exist);

                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    return null;
                }

            });
        } catch (Exception e) {
            e.printStackTrace();
        }finally {

        }
    }


    public static void main(String args[])
            throws IOException,
            InterruptedException{

        //HbaseCli cli = new HbaseCli("hdfs://192.168.128.141:8020/hbase","192.168.128.143","2181");
        //HbaseCli cli = HbaseCli.getInstance("bigdata012020,bigdata012011,bigdata012013,bigdata012012,bigdata012021","2181","/hbase");
        HbaseCli cli = HbaseCli.getInstance("bigdata014231,bigdata014232,bigdata014233","2181","/hbase");
        System.out.println("begin.....");
        //cli.getNameSpace();
        List<Object> objects = cli.getTableNamesByNamespace("eda");
        System.out.println("objects = "+objects);

        cli.createTable();
        System.out.println("end.....");
    }


    public void setConnectSuccess(boolean isConnectSuccess) {
        this.isConnectSuccess = isConnectSuccess;
    }


    public boolean isConnectSuccess() {
        return isConnectSuccess;
    }

    public HConnection getConn() {
        return conn;
    }

    public void setConn(HConnection conn) {
        HbaseCli.conn = conn;
    }

    public Configuration getConf() {
        return conf;
    }

    public void setConf(Configuration conf) {
        HbaseCli.conf = conf;
    }
}






