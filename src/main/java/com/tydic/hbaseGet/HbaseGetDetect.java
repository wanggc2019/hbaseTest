package com.tydic.hbaseGet;

//package com.test;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Random;
import java.util.ResourceBundle;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

public class HbaseGetDetect {
    public static String getTableName = null;

    public static String rowkeyFileName = null;

    public static int detectCnts = 0;

    public static List<String> lstRowkey = new ArrayList<>();

    public static Connection connection = null;

    public static boolean printLog;

    public static String zkQuorum;

    public static String keytab;

    public static String principal;

    public static int sleepSec;

    static {
        ResourceBundle resourceBundle = ResourceBundle.getBundle("hb");
        printLog = resourceBundle.getString("printLog").equals("1");
        zkQuorum = resourceBundle.getString("zkQuorum");
        resourceBundle.getString("sleepSec");
        sleepSec = Integer.parseInt(resourceBundle.getString("sleepSec"));
        System.out.println("zkQuorum:[" + zkQuorum + "],sleepSec=[" + sleepSec + "]");
    }

    public static void main(String[] args) throws IOException, Exception {
        if (args.length < 3) {
            System.out.println("usage HbaseGetDetect TableName rowkeyFileName detectCnts");
            System.exit(2);
        }
        getTableName = args[0];
        rowkeyFileName = args[1];
        detectCnts = Integer.parseInt(args[2]);
/*        getTableName = "test" ;
        rowkeyFileName = "rowkey.txt";
        detectCnts = Integer.parseInt("10") ;*/
        System.out.println("getTableName:[" + getTableName + "], rowkeyFileName=[" + rowkeyFileName + "]");
        if (init() < 0)
            System.exit(2);
        doDetect();
        destroy();
    }

    public static void destroy() {
        if (connection != null)
            try {
                connection.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
    }

    public static int init() {
        System.out.println("-----enter  init ----- ");
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", zkQuorum);
        conf.set("hbase.client.retries.number", "1");

        try {
            connection = ConnectionFactory.createConnection(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
        try {
            FileInputStream in = new FileInputStream(rowkeyFileName);
            InputStreamReader inReader = new InputStreamReader(in, StandardCharsets.UTF_8);
            BufferedReader bufReader = new BufferedReader(inReader);
            String line = null;
            int i = 1;
            while ((line = bufReader.readLine()) != null) {
                lstRowkey.add(line);
                i++;
            }
            bufReader.close();
            inReader.close();
            in.close();
            System.out.println("rowkey sample counts:" + lstRowkey.size());
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("read " + rowkeyFileName + " error!");
        }
        return 0;
    }

    public static void doDetect() throws Exception, IOException {
        int rowkeyCnts = lstRowkey.size();
        int MAX = rowkeyCnts - 1;
        int MIN = 0;
        Random rand = new Random();
        int randomIdx = 0;
        Table table = connection.getTable(TableName.valueOf(getTableName));
        if (printLog)
            System.out.println("**get table ok !");
        double time = 0.0D;
        String columnFamily = null;
        String columnName = null;
        String columnValue = null;
        int columnCnt = 0;
        for (int i = 0; i < detectCnts; i++) {
            long start = System.currentTimeMillis();
            randomIdx = rand.nextInt(MAX - MIN + 1) + MIN;
            String key = lstRowkey.get(randomIdx);
            Get get = new Get(Bytes.toBytes(key));
            Result result = table.get(get);
            if (!result.isEmpty()) {
                List<Cell> listCells = result.listCells();
                columnCnt = 0;
                for (Cell cell : listCells) {
                    columnFamily = Bytes.toString(CellUtil.cloneFamily(cell));
                    columnName = Bytes.toString(CellUtil.cloneQualifier(cell));
                    columnValue = Bytes.toString(CellUtil.cloneValue(cell));
                    columnCnt++;
                }
            }
            table.close();
            long end = System.currentTimeMillis();
            time = (end - start) / 1000.0D;
            System.out.println(new Date() + "###" + i + "###  getTableName:[" + getTableName + "],key:[" + key + "], time:[" + time + "] second" + ",columnCnt:[" + columnCnt + "]");
            Thread.sleep((sleepSec * 1000));
        }
        System.out.println("**hbase query finished !**");
    }
}

