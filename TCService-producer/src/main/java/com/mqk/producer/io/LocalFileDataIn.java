package com.mqk.producer.io;

import com.mqk.common.bean.Data;
import com.mqk.common.bean.DataSource;
import java.io.*;
import java.util.ArrayList;
import java.util.List;

/**
 * 本地文件数据输入
 */
public class LocalFileDataIn implements DataSource {

	private BufferedReader reader = null;

	@Override
	public Object read() throws IOException {
		return null;
	}

	@Override
	public <T extends Data> List<T> read(Class<T> clazz) throws IOException {
		List<T> ts = new ArrayList<>();
		//从文件中读取所有数据
		String line;
		while ((line = reader.readLine()) != null){
			try {
				T t = clazz.newInstance();
				t.setValue(line);
				ts.add(t);
			} catch (InstantiationException e) {
				e.printStackTrace();
			} catch (IllegalAccessException e) {
				e.printStackTrace();
			}
		}
		return ts;
	}

	@Override
	public void close() throws IOException {
		if(null != reader){
			reader.close();
		}

	}

	@Override
	public void setPath(String path) {
		try {
			reader = new BufferedReader(new InputStreamReader(new FileInputStream(path),"UTF-8"));
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}

	}

	public LocalFileDataIn(String path) {
		setPath(path);
	}

}
