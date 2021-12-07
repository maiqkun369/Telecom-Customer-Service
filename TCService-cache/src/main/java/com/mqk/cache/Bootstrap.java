package com.mqk.cache;

import com.mqk.common.utils.JDBCUtil;
import redis.clients.jedis.Jedis;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

/**
 * 缓存服务
 */
public class Bootstrap {
	public static void main(String[] args) {
		//读取mysql数据
		Map<String, Integer> userMap = new HashMap<>();
		Map<String, Integer> dateMap = new HashMap<>();

		//读取用户，时间数据
		Connection connection = null;
		PreparedStatement preparedStatement = null;
		ResultSet rs = null;
		try {
			//获取资源
			connection = JDBCUtil.getConnection();
			//查询用户
			String sqlForQueryUser = "select id, tel from ct_user";
			preparedStatement = connection.prepareStatement(sqlForQueryUser);
			rs = preparedStatement.executeQuery();
			while (rs.next()){
				int id = rs.getInt(1);
				String tel = rs.getString(2);
				userMap.put(tel, id);
			}
			rs.close();

			//查询时间
			String sqlForQueryDate = "select id, year, month, day from ct_date";
			preparedStatement = connection.prepareStatement(sqlForQueryDate);
			rs = preparedStatement.executeQuery();
			while (rs.next()){
				final int id = rs.getInt(1);
				final String year = rs.getString(2);
				String month = rs.getString(3);
				if(month.length() == 1){
					month = "0" + month;
				}
				String day = rs.getString(4);
				if(day.length() == 1){
					day = "0" + day;
				}
				dateMap.put(year + month + day, id);
			}
			rs.close();

		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			if(null != rs){
				try {
					rs.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
			if(null != preparedStatement){
				try {
					preparedStatement.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
			if(null != connection){
				try {
					connection.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}

		//存到redis
		final Jedis jedis = new Jedis("hadoop102",6379);
		for (String key: userMap.keySet()) {
			jedis.hset("ct_user", key, userMap.get(key).toString());
		}

		for (String key: dateMap.keySet()) {
			jedis.hset("ct_date", key, dateMap.get(key).toString());
		}



	}
}
