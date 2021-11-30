package com.mqk.common.bean;

import java.io.Closeable;
import java.io.IOException;

/**
 * 生产数据
 */
public interface Producer extends Closeable {
	/**
	 * 生产数据
	 */
	void produce() throws IOException;

	/**
	 * 数据源头
	 * @param dataSource
	 */
	void setSource(DataSource dataSource);

	/**
	 * 数据流向
	 * @param dataStream
	 */
	void setOut(DataStream dataStream);


}
