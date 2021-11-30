package com.mqk.common.utils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class DateUtil {
	public static Date parse(String dateString, String format){
		final SimpleDateFormat sdf = new SimpleDateFormat(format);
		Date parse = null;
		try {
			parse = sdf.parse(dateString);
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return parse;
	}

	public static String format(Date date, String format){
		final SimpleDateFormat sdf = new SimpleDateFormat(format);
		return sdf.format(date);
	}
}
