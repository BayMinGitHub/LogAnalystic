package com.qianfeng.util;

import com.qianfeng.common.GlobalConstants;

import java.sql.*;

/**
 * @Description: JDBC工具类, 获取MySQL的连接和关闭
 * Author by BayMin, Date on 2018/7/27.
 */
public class JDBCUtil {
    static {
        try {
            Class.forName(GlobalConstants.DRIVER);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("找不到Mysql驱动类");
        }
    }

    /**
     * 获取MySQL连接
     */
    public static Connection getConn() {
        Connection conn = null;
        try {
            // conn = DriverManager.getConnection("jdbc:mysql://hadoop010:3306/report", "root", "1995011");
            conn = DriverManager.getConnection(GlobalConstants.URL, GlobalConstants.USERNAME, GlobalConstants.PASSWORD);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return conn;
    }

    /**
     * 关闭相关对象
     */
    public static void close(Connection conn, PreparedStatement ps, ResultSet rs) {
        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException e) {
                // do nothing
            }
        }
        if (ps != null) {
            try {
                ps.close();
            } catch (SQLException e) {
                // do nothing
            }
        }
        if (rs != null) {
            try {
                rs.close();
            } catch (SQLException e) {
                // do nothing
            }
        }
    }

    // 测试
    // public static void main(String[] args) {
    //     System.out.println(getConn());
    // }
}
