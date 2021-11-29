package com.mqk.producer;

import com.mqk.common.bean.Producer;
import com.mqk.producer.bean.LocalFileProducer;
import com.mqk.producer.io.LocalFileDataIn;
import com.mqk.producer.io.LocalFileDataOut;

import java.io.IOException;

/**
 * 启动对象
 */
public class Bootstrap {
	public static void main(String[] args) throws IOException {
		if(args.length < 2){
			System.out.println("wrong args !!!");
			System.exit(1);
		}

		Producer producer = new LocalFileProducer();
		producer.setSource(new LocalFileDataIn(args[0]));
		producer.setOut(new LocalFileDataOut(args[1]));

		//生产数据
		producer.produce();

		//关闭
		producer.close();
	}
}
