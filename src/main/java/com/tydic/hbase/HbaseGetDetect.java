package com.tydic.hbase;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;

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
import org.apache.hadoop.security.UserGroupInformation;

//hbase的rowkey拨测程序
public class HbaseGetDetect {

	public static String getTableName = null ;
	public static String rowkeyFileName = null ;
	public static int detectCnts = 0;
	public static List<String> lstRowkey = new ArrayList<String>();

	//创建一个单例
	public static Connection connection = null ;

	//配置参数
	public static boolean printLog ;
	public static String zkQuorum ;
	public static String keytab ;
	public static String principal ;
	public static int sleepSec ;

	public HbaseGetDetect() {
	}

	//从配置文件加载
	static {
		//Locale locale = new Locale("en", "US");
		ResourceBundle resourceBundle = ResourceBundle.getBundle("hb");

		printLog = resourceBundle.getString("printLog").equals("1") ? true:false ;
		zkQuorum = resourceBundle.getString("zkQuorum");
		keytab = resourceBundle.getString("keytab");
		principal = resourceBundle.getString("principal");
		sleepSec = Integer.parseInt(resourceBundle.getString("sleepSec")!=null ? resourceBundle.getString("sleepSec"): "1");

		System.out.println("==============zkQuorum:[" + zkQuorum + "]" + "=============================");
		System.out.println("==============keytab:[" + keytab + "]"+ "=============================");
		System.out.println("==============principal:[" + principal + "]"+ "=============================");
		System.out.println("==============sleepSec:["  + sleepSec + "]"+ "=============================");
	}


	public static void main(String[] args) throws IOException, Exception {
/*
		if(args.length<3) {
			System.out.println("usage HbaseGetDetect TableName rowkeyFileName detectCnts");
			System.exit(2);
		}
*/

		/*getTableName = args[0] ;
		rowkeyFileName = args[1] ;
		detectCnts = Integer.parseInt(args[2]) ;

 */
		getTableName = "eda:hive_create_hbase" ;
		rowkeyFileName = "rowkey.txt";
		detectCnts = Integer.parseInt("10") ;

		System.out.println("========================getTableName:[" + getTableName + "]"+ "========================" );
		System.out.println("========================rowkeyFileName:[" + rowkeyFileName + "]"+ "========================" );
		System.out.println("========================detectCnts:[" + detectCnts + "]"+ "========================" );
		//初始化
		if(init()<0)
			//非正常退出
			System.exit(2);

		//查询
		doDetect();

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

		System.out.println("======================= start login hbase ==============================");
		System.out.println("========================= init Kerberos ==============================");

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
		System.out.println("================= hbase login sucess! ===============================");

		//实例化出来connection单例
		try {
			connection = ConnectionFactory.createConnection(conf);
		} catch (IOException e) {
			e.printStackTrace();
		}

		//读取键值文件
		try {
			FileInputStream in = new FileInputStream(rowkeyFileName);
			InputStreamReader inReader = new InputStreamReader(in, "UTF-8");
			BufferedReader bufReader = new BufferedReader(inReader);
			String line = null;
			int i = 1;
			while((line = bufReader.readLine()) != null){
				//System.out.println("第" + i + "行: [" + line + "]");
				lstRowkey.add(line);
				i++;
			}
			bufReader.close();
			inReader.close();
			in.close();

			System.out.println("============================rowkey sample counts:" + lstRowkey.size()+"==============================================");

		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("===========================read " + rowkeyFileName + " error！"+"=========================================");
		}

		return 0 ;
	}

	//开始探测
	public static void doDetect() throws Exception, IOException {

		//传入参数
		int rowkeyCnts = lstRowkey.size();

		//随机
		int MAX = rowkeyCnts-1 ;
		int MIN = 0;
		Random rand = new Random();
		int randomIdx = 0 ;

		//创建表对象
		Table table = connection.getTable(TableName.valueOf(getTableName));

		if(printLog) System.out.println("============================get table name is : "+getTableName+"========================================");

		long start,end ;
		double time = 0;

		String columnFamily = null;
		String columnName = null;
		String columnValue = null;
		int columnCnt=0;

		//循环探测
		for(int i=0;i< detectCnts ; i++) {

			start = System.currentTimeMillis();

			//选择rowkey
			randomIdx = rand.nextInt(MAX - MIN + 1) + MIN ;

			//查询条件
			String key = lstRowkey.get(randomIdx) ;
			Get get = new Get(Bytes.toBytes(key));

			//提取结果
			Result result = table.get(get);
			if(!result.isEmpty()) {

				List<Cell> listCells = result.listCells();
				columnCnt =0;

				for (Cell cell : listCells) {
					columnFamily = Bytes.toString(CellUtil.cloneFamily(cell));
					columnName = Bytes.toString(CellUtil.cloneQualifier(cell));
					columnValue = Bytes.toString(CellUtil.cloneValue(cell));

					//System.out.println("columnFamily:" + columnFamily + ",columnName:" + columnName );
					columnCnt++;
				}
			}
			table.close();

			end = System.currentTimeMillis() ;

			time = (end-start)/1000.00 ;

			System.out.println( new Date() + "###" + i + "###  getTableName:["  +  getTableName + "],key:[" + key + "], time:[" + time + "] second" + ",columnCnt:[" + columnCnt + "]" );

			//短暂停顿
			Thread.sleep(sleepSec*1000) ;
		}

		System.out.println("=================================hbase query finished !================================================");
	}
}

/*
* HADOOP_HOME=/opt/cloudera/parcels/CDH/lib/hadoop
HBASE_HOME=/opt/cloudera/parcels/CDH/lib/hbase

LIBJARS=.
for jar in $HADOOP_HOME/hadoop-*.jar \
           $HBASE_HOME/hbase*.jar $HBASE_HOME/lib/*.jar  ; do \
    LIBJARS=$LIBJARS:$jar
done

echo $LIBJARS


JAVAMEMOPTIONS="-Xms16m -Xmx256m"

#执行应用class
java $JAVAMEMOPTIONS -cp $LIBJARS com.test.HbaseGetDetect $@
* */