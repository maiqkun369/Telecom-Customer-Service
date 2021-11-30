package com.mqk.consumer.hbase;

import com.mqk.common.bean.BaseHbaseDao;
import com.mqk.common.constant.Names;
import com.mqk.common.constant.ValueConstant;
import com.mqk.consumer.bean.Calllog;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;


public class HbaseDao extends BaseHbaseDao {

	/**
	 * 初始化
	 */
	public void init() throws IOException {
		//初始化hbase连接
		start();

		createNamespaceNx(Names.NAMESPACE.getValue());

		createTableXX(Names.TABLE.getValue(),
				ValueConstant.REGION_COUNT, Names.CF_CALLER.getValue());

		//关闭连接
		end();

	}

	/**
	 * 插入对象
	 * @param calllog
	 * @throws Exception
	 */
	public void insertData(Calllog calllog) throws Exception{
		String rk = genRegionNum(calllog.getCall1(), calllog.getCalltime()) + "_"
				+ calllog.getCall1() +  "_" + calllog.getCalltime() + "_" +
				calllog.getCall2() + "_" + calllog.getDuration();
		calllog.setRowkey(rk);
		putData(calllog);

	}

	/**
	 * 插入数据
	 * @param value
	 */
	public void insertData(String value) throws IOException {
		//通话日志保存到hbase表中
		final String[] split = value.split("\t");
		String call1 = split[0];
		String call2 = split[1];
		String callTime = split[2];
		String duration = split[3];

		/*
		rowkey 设计原则：
			1.长度原则（最好是8的倍数能短则短， 范围在10~100 byte）
			2.唯一原则，
		    3.散列性（盐值三列，不能直接用时间戳作为rowkey, 非得用的话包括电话号码，
		    可以使用字符串翻转操作, 最好自己根据预分区的区间，计算分区号）
		 */

		// rk = regionNum + call1 + time + call2 + duration
		String rk = genRegionNum(call1, callTime) + "_" + call1 +  "_" + callTime + "_" + call2 + "_" + duration;

		final Put put = new Put(Bytes.toBytes(rk));
		put.addColumn(
				Bytes.toBytes(Names.CF_CALLER.getValue()),
				Bytes.toBytes("call1"),
				Bytes.toBytes(call1)
		);
		put.addColumn(
				Bytes.toBytes(Names.CF_CALLER.getValue()),
				Bytes.toBytes("call2"),
				Bytes.toBytes(call2)
		);
		put.addColumn(
				Bytes.toBytes(Names.CF_CALLER.getValue()),
				Bytes.toBytes("callTime"),
				Bytes.toBytes(callTime)
		);
		put.addColumn(
				Bytes.toBytes(Names.CF_CALLER.getValue()),
				Bytes.toBytes("duration"),
				Bytes.toBytes(duration)
		);
		putData(Names.TABLE.getValue(), put);


	}



}
