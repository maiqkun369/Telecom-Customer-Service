package com.mqk.analysis.mapper;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;

import java.io.IOException;

/**
 * 分析数据Mapper
 */
public class AnalysisTextMapper extends TableMapper<Text, Text> {
	@Override
	protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {

		//此处的key就是hbase表的rk
		final String rowKey = Bytes.toString(key.get());
		final String[] values = rowKey.split("_");


		String call1 = values[1];
		String call2 = values[3];
		String callTime = values[2];
		String duration = values[4];

		String year = callTime.substring(0, 4);
		String month = callTime.substring(0, 6);
		String day = callTime.substring(0, 8);

		//  主叫用户 - 年
		context.write(new Text(call1 + "_" + year), new Text(duration));
		//  主叫用户 - 月
		context.write(new Text(call1 + "_" + month), new Text(duration));
		//  主叫用户 - 日
		context.write(new Text(call1 + "_" + day), new Text(duration));

		//  被叫用户 - 年
		context.write(new Text(call2 + "_" + year), new Text(duration));
		//  被叫用户 - 月
		context.write(new Text(call2 + "_" + month), new Text(duration));
		//  被叫用户 - 日
		context.write(new Text(call2 + "_" + day), new Text(duration));
	}
}
