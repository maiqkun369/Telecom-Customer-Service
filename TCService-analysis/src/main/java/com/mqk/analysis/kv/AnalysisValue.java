package com.mqk.analysis.kv;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class AnalysisValue implements Writable {
	private String sumCall;
	private String sumDuration;

	public AnalysisValue() {
	}

	public AnalysisValue(String sumCall, String sumDuration) {
		this.sumCall = sumCall;
		this.sumDuration = sumDuration;
	}

	/**
	 * 序列化
	 * @param dataOutput
	 * @throws IOException
	 */
	@Override
	public void write(DataOutput dataOutput) throws IOException {
		dataOutput.writeUTF(this.sumCall);
		dataOutput.writeUTF(this.sumDuration);

	}

	/**
	 * 反序列化
	 * @param dataInput
	 * @throws IOException
	 */
	@Override
	public void readFields(DataInput dataInput) throws IOException {
		this.sumCall = dataInput.readUTF();
		this.sumDuration = dataInput.readUTF();
	}

	public String getSumCall() {
		return sumCall;
	}

	public void setSumCall(String sumCall) {
		this.sumCall = sumCall;
	}

	public String getSumDuration() {
		return sumDuration;
	}

	public void setSumDuration(String sumDuration) {
		this.sumDuration = sumDuration;
	}
}
