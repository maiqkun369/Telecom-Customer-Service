package com.mqk.common.bean;


import com.mqk.common.api.Column;
import com.mqk.common.api.Rowkey;
import com.mqk.common.api.TableRef;
import com.mqk.common.constant.Names;
import com.mqk.common.constant.ValueConstant;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

/**
 * 基础 dao
 */
public abstract class BaseHbaseDao {
	/**
	 * 在线程粒度下复用conn连接对象
	 */
	private ThreadLocal<Connection> connHolder = new ThreadLocal<>();

	/**
	 * 在线程粒度下复用conn连接对象
	 */
	private ThreadLocal<Admin> adminHolder = new ThreadLocal<>();


	/**
	 * 插入数
	 * @param calllog
	 */
	protected void putData(Object calllog) throws Exception{
		//反射对象
		final Class<?> aClass = calllog.getClass();

		final TableRef tableRef = aClass.getAnnotation(TableRef.class);

		//tableRef中可以拿到注解的相关信息，此处拿出value的表名
		String tableName = tableRef.value();

		//拿到该注解标注类下的所有属性
		final Field[] fs = aClass.getDeclaredFields();
		//rowkey
		String stringRk = "";

		//遍历找到rk
		for (Field f : fs) {
			//找出所有属性中有Rowkey注解的属性
			final Rowkey rowkey = f.getAnnotation(Rowkey.class);
			if(null != rowkey){
				//因为属性是私有的，此处要设置accessible
				f.setAccessible(true);
				stringRk = (String) f.get(calllog);
				//找了想要的rk就出循环
				break;
			}
		}

		final Connection connection = getConnection();
		//获取表
		final Table table = connection.getTable(TableName.valueOf(tableName));
		final Put put = new Put(Bytes.toBytes(stringRk));


		//遍历添加列
		for (Field f : fs) {
			final Column col = f.getAnnotation(Column.class);
			//排除掉类中没加@Column注解的字段
			if(null != col){
				//列族
				String family = col.family();
				//列
				String column = col.column();

				if(null == column || "".equals(column)){
					//没有在@Column注解中没有指定column值的话就用当前属性的名字
					column = f.getName();
				}

				//私有属性要设置ACCESS=TRUE
				f.setAccessible(true);
				//属性的值
				String value = (String) f.get(calllog);

				//添加数据
				put.addColumn(
						Bytes.toBytes(family),
						Bytes.toBytes(column),
						Bytes.toBytes(value)
				);
			}
		}

		//增加数据
		table.put(put);

		table.close();

	}


	protected void putData(String tableName, Put put) throws IOException {
		final Connection connection = getConnection();
		//获取表
		final Table table = connection.getTable(TableName.valueOf(tableName));
		table.put(put);
		table.close();
	}

	/**
	 * 创建名称空间
	 */
	protected void createNamespaceNx(String namespace) throws IOException {
		final Admin admin = getAdmin();

		try {
			admin.getNamespaceDescriptor(namespace);
		} catch (NamespaceNotFoundException e){
			//不存在namespace 则创建
			final NamespaceDescriptor namespaceDescriptor =
					NamespaceDescriptor.create(namespace).build();
			admin.createNamespace(namespaceDescriptor);
		}
	}

	/**
	 * 创建表
	 */
	protected void createTableXX(String tableName, String... cfs) throws IOException {
		createTableXX(tableName, null, cfs);
	}

	protected void createTableXX(String tableName, Integer regionCount, String... cfs) throws IOException {
		final Admin admin = getAdmin();

		if(admin.tableExists(TableName.valueOf(tableName))){
			//存在，删除后在创建
			deleteTable(tableName);
		}

		//不存在,创建
		createTable(tableName, regionCount, cfs);
	}

	private void createTable(String tableName, Integer regionCount, String... cfs) throws IOException {
		final Admin admin = getAdmin();
		HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
		if(null == cfs || cfs.length == 0){
			cfs = new String[1];
			cfs[0] = Names.CF_INFO.getValue();
		}

		for (String cf : cfs) {
			final HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(cf);
			tableDescriptor.addFamily(hColumnDescriptor);
		}

		if(null == regionCount || regionCount <= 1){
			//不用做预分区
			admin.createTable(tableDescriptor);
		}else {
			//做预分区
			//分区键
			byte[][] spiltKeys = genSplitKeys(regionCount);
			admin.createTable(tableDescriptor, spiltKeys);
		}

	}

	/**
	 * 生成分区键
	 * @param regionCount
	 * @return
	 */
	private byte[][] genSplitKeys(Integer regionCount){

		/*
		  注意分区键：
		    (-无穷，0|) [0|,1|), [1|, +无穷)
			0|的ASCALL码大于0, 如上的写法可以保证
			以0开头的字符串落在第一个分区内，以1开头的一定会落在第二个区间
		 */
		int splitkeyCount = regionCount - 1;
		byte[][] bs = new byte[splitkeyCount][];

		List<byte[]> bsList = new ArrayList<>();
		for (int i = 0; i < splitkeyCount; i++) {
			String splitKey = i + "|";
			System.out.println(splitKey);
			bsList.add(Bytes.toBytes(splitKey));
		}

		bsList.toArray(bs);
		return bs;
	}

	/**
	 * 计算分区号
	 * @param tel
	 * @param date
	 * @return
	 */
	protected int genRegionNum(String tel, String date){

		String userCode = tel.substring(tel.length() - 4);
		String yearMonth = date.substring(0, 6);

		int userCodeHash =  userCode.hashCode();
		int yearMonthHash = yearMonth.hashCode();

		//crc采用异或算法
		int crc = Math.abs(userCodeHash ^ yearMonthHash);

		//取模
		int regionNum = crc % ValueConstant.REGION_COUNT;

		return regionNum;

	}


	protected void deleteTable(String tableName) throws IOException {
		final Admin admin = getAdmin();
		admin.disableTable(TableName.valueOf(tableName));
		admin.deleteTable(TableName.valueOf(tableName));
	}
	/**
	 * 获取连接
	 */
	protected synchronized Connection getConnection() throws IOException {
		//存在则直接拿出来用
		Connection conn = this.connHolder.get();
		if(null == conn){
			//不存在则创建
			final Configuration conf = HBaseConfiguration.create();
			//集群信息
			conf.set("hbase.zookeeper.quorum", "hadoop102,hadoop103,hadoop104");
			conn = ConnectionFactory.createConnection(conf);
			this.connHolder.set(conn);
		}
		return conn;
	}
	/**
	 * 获取admin
	 */
	protected synchronized Admin getAdmin() throws IOException {
		//存在则直接拿出来用
		Admin admin = this.adminHolder.get();
		if(null == admin){
			//不存在则创建
			admin = getConnection().getAdmin();
			this.adminHolder.set(admin);
		}
		return admin;
	}

	protected void start() throws IOException {
		getConnection();
		getAdmin();
	}
	protected void end() throws IOException {
		final Admin admin = getAdmin();
		if(null != admin){
			admin.close();
			this.adminHolder.remove();
		}
		final Connection connection = getConnection();
		if(null != connection){
			connection.close();
			this.connHolder.remove();
		}
	}
}
