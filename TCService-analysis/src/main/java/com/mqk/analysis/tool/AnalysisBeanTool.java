package com.mqk.analysis.tool;

import com.mqk.analysis.io.MysqlBeanOutputFormat;
import com.mqk.analysis.kv.AnalysisKey;
import com.mqk.analysis.kv.AnalysisValue;
import com.mqk.analysis.mapper.AnalysisBeanMapper;
import com.mqk.analysis.reducer.AnalysisBeanReducer;
import com.mqk.common.constant.Names;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.util.Tool;

/**
 * 分析数据工具
 */
public class AnalysisBeanTool implements Tool {
	@Override
	public int run(String[] args) throws Exception {
		final Job job = Job.getInstance();
		job.setJarByClass(AnalysisBeanTool.class);

		final Scan scan = new Scan();
		//指定列族
		scan.addFamily(Bytes.toBytes(Names.CF_CALLER.getValue()));

		//mapper mr对于hbase读取数据封装的工具类
		TableMapReduceUtil.initTableMapperJob(
				Names.TABLE.getValue(),
				scan,
				AnalysisBeanMapper.class,
				AnalysisKey.class,
				Text.class,
				job
		);

		//reducer
		job.setReducerClass(AnalysisBeanReducer.class);
		job.setOutputKeyClass(AnalysisKey.class);
		job.setOutputValueClass(AnalysisValue.class);

		//outputformatter
		job.setOutputFormatClass(MysqlBeanOutputFormat.class);

		final boolean flag = job.waitForCompletion(true);
		if(flag){
			return JobStatus.State.SUCCEEDED.getValue();
		}else {
			return JobStatus.State.FAILED.getValue();
		}
	}

	@Override
	public void setConf(Configuration configuration) {

	}

	@Override
	public Configuration getConf() {
		return null;
	}
}
