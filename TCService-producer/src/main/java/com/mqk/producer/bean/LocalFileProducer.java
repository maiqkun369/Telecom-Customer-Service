package com.mqk.producer.bean;

import com.mqk.common.bean.DataSource;
import com.mqk.common.bean.DataStream;
import com.mqk.common.bean.Producer;
import com.mqk.common.utils.DateUtil;
import com.mqk.common.utils.NumberUtil;

import java.io.IOException;
import java.util.Date;
import java.util.List;
import java.util.Random;

/**
 * 数据文件生产者
 */
public class LocalFileProducer implements Producer {
	private DataSource dataSource;
	private DataStream dataStream;
	private volatile boolean flag = true;



	@Override
	public void setSource(DataSource dataSource) {
		this.dataSource = dataSource;

	}

	@Override
	public void setOut(DataStream dataStream) {
		this.dataStream = dataStream;
	}

	@Override
	public void produce() {
		//通讯录数据
		try {
			List<Contact> contacts = dataSource.read(Contact.class);
			while (flag){
				//通讯录中随机查找两个电话号码
				int call1Index = new Random().nextInt(contacts.size());
				int call2Index;
				//保障两个索引不相等
				while (true){
					call2Index = new Random().nextInt(contacts.size());
					if(call1Index != call2Index){
						break;
					}
				}

				//主叫与被叫
				final Contact call1 = contacts.get(call1Index);
				final Contact call2 = contacts.get(call2Index);

				//生成通话时间
				String  startDate = "20180101000000";
				String  endDate = "20190101000000";

				long startTime = DateUtil.parse(startDate, "yyyyMMddHHmmss").getTime();
				long endTime = DateUtil.parse(endDate, "yyyyMMddHHmmss").getTime();

				long callTime = startTime + (long)((endTime - startTime) * Math.random());

				final String callTimeString = DateUtil.format(new Date(callTime), "yyyyMMddHHmmss");

				//生成通话时长
				final String duration =
						NumberUtil.format(new Random().nextInt(3000), 4);

				//生成通话记录
				final CallLog log = new CallLog(
						call1.getTel(),
						call2.getTel(),
						callTimeString,
						duration);
				//将记录刷写到数据文件中
				dataStream.write(log);
				Thread.sleep(500);
			}
		} catch (IOException | InterruptedException e) {
			e.printStackTrace();
		}




	}

	/**
	 * 关闭生产者
	 * @throws IOException
	 */
	@Override
	public void close() throws IOException {
		if(null != dataSource){
			dataSource.close();
		}
		if(null != dataStream){
			dataStream.close();
		}
	}
}
