package com.mqk.producer.bean;

import com.mqk.common.bean.Data;

/**
 * 联系人实体
 */
public class Contact extends Data {
	private String tel;
	private String name;

	public String getTel() {
		return tel;
	}

	public void setTel(String tel) {
		this.tel = tel;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	@Override
	public void setValue(Object value) {
		content = (String) value;
		final String[] values = content.split("\t");
		setName(values[1]);
		setTel(values[0]);
	}

	@Override
	public String toString() {
		final StringBuffer sb = new StringBuffer("Contact{");
		sb.append("tel='").append(tel).append('\'');
		sb.append(", name='").append(name).append('\'');
		sb.append('}');
		return sb.toString();
	}
}
