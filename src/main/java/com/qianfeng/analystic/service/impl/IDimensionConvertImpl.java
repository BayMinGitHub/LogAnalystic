package com.qianfeng.analystic.service.impl;

import com.qianfeng.analystic.model.dim.base.*;
import com.qianfeng.analystic.service.IDimensionConvert;
import com.qianfeng.util.JDBCUtil;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.sql.*;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * @Description: 操作维度表的接口实现
 * Author by BayMin, Date on 2018/7/27.
 */
public class IDimensionConvertImpl implements IDimensionConvert {
    private static final Logger logger = Logger.getLogger(IDimensionConvertImpl.class);
    // 用于存储维度:维度累计的SQL个数
    public Map<String, Integer> batch = new HashMap<>();
    // 维度:维度对应的id 缓存
    private Map<String, Integer> cache = new LinkedHashMap<String, Integer>() {
        @Override
        protected boolean removeEldestEntry(Map.Entry<String, Integer> eldest) {
            return this.size() > 1000;
        }
    };

    /**
     * 获取维度id
     * 0.先查询缓存中是否存在对应维度,如果有直接取出返回
     * 1.先用根据维度属性去查询数据库,如果有就返回维度对应的ID
     * 2.如果没有先插入先返回
     */
    @Override
    public int getDimensionIDByDimension(BaseDimension baseDimension) throws IOException, SQLException {
        String cacheKey = this.buildCache(baseDimension);
        if (this.cache.containsKey(cacheKey))
            return this.cache.get(cacheKey);
        // 代码走到这儿,已确定缓存中没有,去查询数据库
        String[] sql = null;
        if (baseDimension instanceof DateDimension)
            sql = this.buildDateSql();
        else if (baseDimension instanceof PlatFormDimension)
            sql = this.buildPlatFormSql();
        else if (baseDimension instanceof BrowserDimension)
            sql = this.buildBrowserSql();
        else if (baseDimension instanceof KpiDimension)
            sql = this.buildKpiSql();
        Connection conn = JDBCUtil.getConn();
        int id = -1;
        synchronized (this) {
            id = this.execute(sql, baseDimension, conn);
        }
        // 将获取到的id添加到缓存
        this.cache.put(cacheKey, id);
        return id;
    }

    /**
     * 执行
     */
    private int execute(String[] sql, BaseDimension baseDimension, Connection conn) {
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            // 先查询
            ps = conn.prepareStatement(sql[0]);
            this.setArgs(baseDimension, ps);
            rs = ps.executeQuery();
            if (rs.next()) {
                return rs.getInt("id");
            }
            // 如果代码走到这里说明没有查询到,然后准备插入再取值
            ps = conn.prepareStatement(sql[1], Statement.RETURN_GENERATED_KEYS); // 返回生成的Key
            this.setArgs(baseDimension, ps);
            ps.executeUpdate(); // 返回影响的函数
            rs = ps.getGeneratedKeys(); // 返回GeneratedKey
            if (rs.next())
                return rs.getInt(1);
        } catch (SQLException e) {
            logger.warn("执行维度SQL异常");
        } finally {
            JDBCUtil.close(conn, ps, rs);
        }
        throw new RuntimeException("查询和插入SQL都异常");
    }

    /**
     * 设置参数
     */
    private void setArgs(BaseDimension baseDimension, PreparedStatement ps) {
        try {
            int i = 0;
            if (baseDimension instanceof DateDimension) {
                DateDimension date = (DateDimension) baseDimension;
                ps.setInt(++i, date.getYear());
                ps.setInt(++i, date.getSeason());
                ps.setInt(++i, date.getMonth());
                ps.setInt(++i, date.getWeeek());
                ps.setInt(++i, date.getDay());
                ps.setDate(++i, new Date(date.getCalendar().getTime())); // SQL包Date
                ps.setString(++i, date.getType());
            } else if (baseDimension instanceof PlatFormDimension) {
                PlatFormDimension platform = (PlatFormDimension) baseDimension;
                ps.setString(++i, platform.getPlatformName());
            } else if (baseDimension instanceof BrowserDimension) {
                BrowserDimension browserDimension = (BrowserDimension) baseDimension;
                ps.setString(++i, browserDimension.getBrowserName());
                ps.setString(++i, browserDimension.getBrowserVersion());
            } else if (baseDimension instanceof KpiDimension) {
                KpiDimension kpiDimension = (KpiDimension) baseDimension;
                ps.setString(++i, kpiDimension.getKpiName());
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    /**
     * 构建sqls
     */
    private String[] buildDateSql() {
        String select = "select `id` from `dimension_date` where `year` = ? and `season` = ? and `month` = ? and `week` = ? and `day` = ? and `calendar` = ? and `type` = ?";
        String insert = "insert into `dimension_date` (`year`,`season`,`month`,`week`,`day`,`calendar`,`type`) values (?,?,?,?,?,?,?)";
        return new String[]{select, insert};
    }

    private String[] buildPlatFormSql() {
        String select = "select `id` from `dimension_platform` where `platform_name` = ?";
        String insert = "insert into `dimension_platform` (`platform_name`) values (?)";
        return new String[]{select, insert};
    }

    private String[] buildBrowserSql() {
        String select = "select `id` from `dimension_browser` where `browser_name` = ? and `browser_version` = ?";
        String insert = "insert into `dimension_browser` (`browser_name`,`browser_version`) values (?,?)";
        return new String[]{select, insert};
    }

    private String[] buildKpiSql() {
        String select = "select `id` from `dimension_kpi` where `kpi_name` = ?";
        String insert = "insert into `dimension_kpi` (`kpi_name`) values (?)";
        return new String[]{select, insert};
    }

    private String buildCache(BaseDimension baseDimension) {
        StringBuffer sb = new StringBuffer();
        if (baseDimension instanceof DateDimension) {
            sb.append("date_");
            DateDimension date = (DateDimension) baseDimension;
            sb.append(date.getYear()).append(date.getSeason()).append(date.getMonth()).append(date.getWeeek()).append(date.getDay()).append(date.getType());
        } else if (baseDimension instanceof PlatFormDimension) {
            sb.append("platform_");
            PlatFormDimension platform = (PlatFormDimension) baseDimension;
            sb.append(platform.getPlatformName());
        } else if (baseDimension instanceof BrowserDimension) {
            sb.append("browser_");
            BrowserDimension browserDimension = (BrowserDimension) baseDimension;
            sb.append(browserDimension.getBrowserName()).append(browserDimension.getBrowserVersion());
        } else if (baseDimension instanceof KpiDimension) {
            sb.append("kpi_");
            KpiDimension kpiDimension = (KpiDimension) baseDimension;
            sb.append(kpiDimension.getKpiName());
        }
        return sb.length() == 0 ? null : sb.toString();
    }
}
