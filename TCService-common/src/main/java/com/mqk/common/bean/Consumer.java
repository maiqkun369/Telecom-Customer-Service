package com.mqk.common.bean;

import java.io.Closeable;

public interface Consumer extends Closeable {

	/**
	 * 消费数据
	 */
	void consume() throws Exception;

}
