package com.mqk.common.api;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;


//表示当前的注解只能用在类上
@Target({ElementType.TYPE})

//表示策略，即什么时候用这个注解,RUNTIME表示运行时拿到注解对应的值
@Retention(RetentionPolicy.RUNTIME)
public @interface TableRef {
	String value();

}
