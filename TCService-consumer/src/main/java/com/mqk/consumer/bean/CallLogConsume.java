package com.mqk.consumer.bean;

import com.mqk.common.bean.Consumer;
import com.mqk.common.constant.Names;
import com.mqk.consumer.hbase.HbaseDao;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;

/**
 * 通话日志消费者
 */
public class CallLogConsume implements Consumer {
	@Override
	public void consume() throws Exception {
		Properties properties = new Properties();
		try {
			properties.load(Thread.currentThread()
					.getContextClassLoader().getResourceAsStream("consumer.properties"));
		} catch (IOException e) {
			e.printStackTrace();
		}
		//获取flume采集的数据
		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

		//订阅主题
		consumer.subscribe(Arrays.asList(Names.TOPIC.getValue()));

		//写入HBase
		HbaseDao hbaseDao = new HbaseDao();
		//初始化hbase环境
		try {
			hbaseDao.init();
		} catch (IOException e) {
			e.printStackTrace();
		}

		//消费数据
		while (true) {
			//拉去数据
			final ConsumerRecords<String, String> records = consumer.poll(100);
			for (ConsumerRecord<String, String> record : records) {
				System.out.println(record.value());
				hbaseDao.insertData(record.value());

//				final Calllog calllog = new Calllog(record.value());
//				hbaseDao.insertData(calllog);

			}
		}

	}

	@Override
	public void close() throws IOException {

	}
}
