package com.mqk.common.bean;

import java.io.Closeable;

/**
 * 数据流向
 */
public interface DataStream extends Closeable {
	void setPath(String path);
	void write(Object data);
	void write(String data);
}
