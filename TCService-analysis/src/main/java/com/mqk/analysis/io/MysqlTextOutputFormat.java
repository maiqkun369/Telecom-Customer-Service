package com.mqk.analysis.io;

import com.mqk.common.utils.JDBCUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;


/**
 * mysql数据格式化
 */
public class MysqlTextOutputFormat extends OutputFormat<Text, Text> {

	protected static class MySQLRecordWriter<K,V> extends RecordWriter<Text,Text>{
		private Connection connection;
		public MySQLRecordWriter() {
			//获取资源
			connection = JDBCUtil.getConnection();
		}

		/**
		 * 输出数据
		 * @param key
		 * @param value
		 * @throws IOException
		 * @throws InterruptedException
		 */
		@Override
		public void write(Text key, Text value) throws IOException, InterruptedException {
			final String[] values = value.toString().split("_");
			PreparedStatement preparedStatement = null;
			try {
				String sql = "insert into ct_call (telid, dateid, sumcall, sumduration) values (?, ?, ?, ?)";
				preparedStatement = connection.prepareStatement(sql);
				preparedStatement.setInt(1, 2);
				preparedStatement.setInt(2, 3);
				preparedStatement.setInt(3, Integer.parseInt(values[0]));
				preparedStatement.setInt(4, Integer.parseInt(values[1]));
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
	public RecordWriter<Text, Text> getRecordWriter(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
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
