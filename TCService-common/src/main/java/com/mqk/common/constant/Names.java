package com.mqk.common.constant;

import com.mqk.common.bean.Val;

/**
 * 名称常量
 */
public enum Names implements Val {
	NAMESPACE("ct"),
	TOPIC("ct"),
	TABLE("ct:calllog"),
	CF_CALLER("caller"),
	CF_CALLEE("callee"),
	CF_INFO("info");



	private String name;
	Names(String name){
		this.name = name;
	}

	@Override
	public void setValue(Object value) {
		name = (String) value;
	}

	@Override
	public String getValue() {
		return name;
	}
}
