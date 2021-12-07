package com.mqk.analysis.io;

import com.mqk.analysis.kv.AnalysisKey;
import com.mqk.analysis.kv.AnalysisValue;
import com.mqk.common.utils.JDBCUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import redis.clients.jedis.Jedis;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;


/**
 * mysql数据格式化
 */
public class MysqlBeanOutputFormat extends OutputFormat<AnalysisKey, AnalysisValue> {

	protected static class MySQLRecordWriter<K,V> extends RecordWriter<AnalysisKey,AnalysisValue>{
		private Connection connection;
		private Jedis jedis;


		public MySQLRecordWriter(){
			//获取资源
			connection = JDBCUtil.getConnection();
			jedis = new Jedis("hadoop102",6379);
		}

		/**
		 * 输出数据
		 * @param key
		 * @param value
		 * @throws IOException
		 * @throws InterruptedException
		 */
		@Override
		public void write(AnalysisKey key, AnalysisValue value) throws IOException, InterruptedException {
			PreparedStatement preparedStatement = null;
			try {
				String sql = "insert into ct_call (telid, dateid, sumcall, sumduration) values (?, ?, ?, ?)";
				preparedStatement = connection.prepareStatement(sql);

				preparedStatement.setInt(1, Integer.parseInt(jedis.hget("ct_user", key.getTel())));
				preparedStatement.setInt(2, Integer.parseInt(jedis.hget("ct_date", key.getDate())));
				preparedStatement.setInt(3, Integer.parseInt(value.getSumCall()));
				preparedStatement.setInt(4, Integer.parseInt(value.getSumDuration()));
				preparedStatement.executeUpdate();
			} catch (SQLException e) {
				e.printStackTrace();
			}finally {
				if(null != preparedStatement){
					try {
						preparedStatement.close();
					} catch (SQLException e) {
						e.printStackTrace();
					}
				}
			}

		}

		@Override
		public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
			if(null != connection){
				try {
					connection.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
			if(null != jedis){
				jedis.close();
			}
		}
	}


	/**
	 * 输出数据到mysql
	 * @param taskAttemptContext
	 * @return
	 * @throws IOException
	 * @throws InterruptedException
	 */
	@Override
	public RecordWriter<AnalysisKey, AnalysisValue> getRecordWriter(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
		return new MySQLRecordWriter();
	}

	@Override
	public void checkOutputSpecs(JobContext jobContext) throws IOException, InterruptedException {

	}





	private FileOutputCommitter committer = null;
	public static Path getOutputPath(JobContext jobContext){
		final String s = jobContext.getConfiguration().get(FileOutputFormat.OUTDIR);
		return s == null ? null : new Path(s);
	}
	@Override
	public OutputCommitter getOutputCommitter(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
		if(committer == null){
			Path output = getOutputPath(taskAttemptContext);
			committer = new FileOutputCommitter(output, taskAttemptContext);
		}
		return committer;
	}
}
