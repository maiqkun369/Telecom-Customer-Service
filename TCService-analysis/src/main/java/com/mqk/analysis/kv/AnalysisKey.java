package com.mqk.analysis.kv;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 自定义key
 */
public class AnalysisKey implements WritableComparable<AnalysisKey> {
	private String tel;
	private String date;
	public AnalysisKey() {

	}
	public AnalysisKey(String tel, String date) {
		this.tel = tel;
		this.date = date;
	}

	@Override
	public int compareTo(AnalysisKey key) {
		int res = this.tel.compareTo(key.getTel());
		if(res == 0) {
			res = this.date.compareTo(key.getDate());
		}
		return res;
	}

	/**
	 * 序列化
	 * @param dataOutput
	 * @throws IOException
	 */
	@Override
	public void write(DataOutput dataOutput) throws IOException {
		dataOutput.writeUTF(this.tel);
		dataOutput.writeUTF(this.date);
	}

	/**
	 * 反序列化
	 * @param dataInput
	 * @throws IOException
	 */
	@Override
	public void readFields(DataInput dataInput) throws IOException {
		this.tel = dataInput.readUTF();
		this.date = dataInput.readUTF();
	}

	public String getTel() {
		return tel;
	}

	public void setTel(String tel) {
		this.tel = tel;
	}

	public String getDate() {
		return date;
	}

	public void setDate(String date) {
		this.date = date;
	}
}
