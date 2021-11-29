package com.mqk.common.bean;


import com.mqk.common.constant.Names;
import com.mqk.common.constant.ValueConstant;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

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
			//存在，删除
			deleteTable(tableName);
		}else {
			//不存在,创建
			createTable(tableName, regionCount, cfs);
		}
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
