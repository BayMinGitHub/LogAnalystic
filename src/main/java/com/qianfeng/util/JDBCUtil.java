package com.qianfeng.util;

import java.sql.*;

/**
 * @Description: JDBC工具类
 * Author by BayMin, Date on 2018/7/27.
 */
public class JDBCUtil {
    public static Connection getConn() {
        Connection conn = null;
        try {
            conn = DriverManager.getConnection("jdbc:mysql://hadoop010:3306/report", "root", "1995011");
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return conn;
    }

    public static void close(Connection conn, PreparedStatement ps, ResultSet rs) {
        if (conn != null) {
            try {
                conn.close();
                ps.close();
                rs.close();
            } catch (SQLException e) {
                // do nothing
            }
        }
    }
}
