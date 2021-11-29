package com.mqk.producer.io;

import com.mqk.common.bean.DataStream;

import java.io.*;

/**
 * 本地文件的数据输出
 */
public class LocalFileDataOut implements DataStream {
	private PrintWriter writer = null;

	public LocalFileDataOut(String path) {
		setPath(path);
	}

	@Override
	public void close() throws IOException {

		if(null != writer){
			writer.close();
		}
	}

	@Override
	public void setPath(String path) {
		try {
			writer = new PrintWriter(new OutputStreamWriter(new FileOutputStream(path),"UTF-8"));
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void write(Object data) {
		write(data.toString());

	}

	/**
	 * 生成到文件中
	 * @param data
	 */
	@Override
	public void write(String data) {
		writer.print(data);
		//来一条刷一条
		writer.flush();
	}
}
