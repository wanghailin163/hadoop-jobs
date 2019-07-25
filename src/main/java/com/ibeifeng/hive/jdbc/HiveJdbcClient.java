package com.ibeifeng.hive.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

public class HiveJdbcClient {
	
	private static String driverName = "org.apache.hive.jdbc.HiveDriver";
	
	public static void main(String[] args) {
		try {
			Class.forName(driverName);
			Connection conn = DriverManager.getConnection("jdbc:hive2://master:10000/db_emp","whl","whl");
			Statement stmt = conn.createStatement();
			String sql = "select ename,deptno from emp";
			System.out.println("Runing:"+sql);
			ResultSet res = stmt.executeQuery(sql);
			while(res.next()){
				System.out.println(res.getString(1)+"\t"+res.getString(2));
			}
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
		
	}

}
