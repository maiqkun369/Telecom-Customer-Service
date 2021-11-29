package com.mqk.consumer;

import com.mqk.common.bean.Consumer;
import com.mqk.consumer.bean.CallLogConsume;

import java.io.IOException;

/**
 * 启动消费者
 */
public class Bootstrap {
	public static void main(String[] args) throws IOException {
		//使用KAFKA消费者获取flume采集的数据
		Consumer consumer = new CallLogConsume();
		//将数据存储到hbase

		consumer.consume();


		consumer.close();

	}
}
