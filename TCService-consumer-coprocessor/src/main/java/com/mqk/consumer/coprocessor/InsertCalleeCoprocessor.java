package com.mqk.consumer.coprocessor;


import com.mqk.common.bean.BaseHbaseDao;
import com.mqk.common.constant.Names;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;


/**
 * 协处理器来增加被叫用户的数据
 *  使用方式：
 *      1.让表关联协处理器类 	tableDescriptor.addCoprocessor(corprocessClassName);
 *      2.将协处理器类打包放到hbase的lib目录下
 */
public class InsertCalleeCoprocessor extends BaseRegionObserver {

	/**
	 * 保存主叫用户数据后，由Hbase自动保存被叫用户数据
	 * @param e 上下文环境
	 * @param put 每次put
	 * @param edit
	 * @param durability
	 * @throws IOException
	 */
	@Override
	public void postPut(ObserverContext<RegionCoprocessorEnvironment> e, Put put, WALEdit edit, Durability durability) throws IOException {
		//拿到表
		Table table = e.getEnvironment().getTable(TableName.valueOf(Names.TABLE.getValue()));

		//主叫用户的rowkey
		final String rkCaller = Bytes.toString(put.getRow());
		final String[] values = rkCaller.split("_");

		final String call1 = values[1];
		final String call2 = values[3];
		final String callTime = values[2];
		final String duration = values[4];
		final String flag = values[5];

		if("1".equals(flag)){//主叫用户才保存对应被叫，被叫用户就不用了
			//用于构建被叫rowkey
			CorprocessDao corprocessDao = new CorprocessDao();
			final String rkCallee = corprocessDao.getRegionNumInCorprocess(call2, callTime) + "_"
					+ call2 + "_" + callTime + "_" + call1 + "_" + duration + "_0";

			final Put calleePut = new Put(Bytes.toBytes(rkCallee));
			calleePut.addColumn(
					Bytes.toBytes(Names.CF_CALLEE.getValue()),
					Bytes.toBytes("call1"),
					Bytes.toBytes(call2)
			);
			calleePut.addColumn(
					Bytes.toBytes(Names.CF_CALLEE.getValue()),
					Bytes.toBytes("call2"),
					Bytes.toBytes(call1)
			);
			calleePut.addColumn(
					Bytes.toBytes(Names.CF_CALLEE.getValue()),
					Bytes.toBytes("callTime"),
					Bytes.toBytes(callTime)
			);
			calleePut.addColumn(
					Bytes.toBytes(Names.CF_CALLEE.getValue()),
					Bytes.toBytes("duration"),
					Bytes.toBytes(duration)
			);
			calleePut.addColumn(
					Bytes.toBytes(Names.CF_CALLEE.getValue()),
					Bytes.toBytes("flag"),
					Bytes.toBytes("0")
			);
			//TODO 此处一定要注意 容易死循环 postput会在put事件后触发
			table.put(calleePut);

			//TODO 不关闭的话有可能会内存溢出，到时regionserver挂掉
			table.close();
		}
	}

	private class CorprocessDao extends BaseHbaseDao{
		public int getRegionNumInCorprocess(String tel, String time){
			return genRegionNum(tel, time);
		}

	}
}
