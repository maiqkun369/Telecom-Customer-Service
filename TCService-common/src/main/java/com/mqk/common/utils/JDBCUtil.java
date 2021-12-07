package com.mqk.common.utils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class JDBCUtil {
	private static final String MYSQL_DRIVER_CLASS = "com.mysql.jdbc.Driver";
	private static final String MYSQL_URL = "jdbc:mysql://hadoop102:3306/tcservice?useUnicode=true&characterEncoding=UTF-8";
	private static final String MYSQL_USERNAME = "root";
	private static final String MYSQL_PASSWORD = "000000";

	public static Connection getConnection(){
		Connection connection = null;
		try {
			Class.forName(MYSQL_DRIVER_CLASS);
			connection = DriverManager.getConnection(MYSQL_URL, MYSQL_USERNAME, MYSQL_PASSWORD);
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
		return connection;
	}
}
