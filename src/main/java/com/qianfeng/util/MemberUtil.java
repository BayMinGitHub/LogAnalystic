package com.qianfeng.util;

import com.qianfeng.common.GlobalConstants;
import com.qianfeng.common.KpiType;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * @Description: 查看会员Id是否是新增会员, 建议过滤不合法的会员Id
 * Author by BayMin, Date on 2018/8/1.
 */
public class MemberUtil {
    // 缓存之前的memberId
    private static Map<String, Boolean> cache = new LinkedHashMap<String, Boolean>() { // Linked是有序的
        @Override
        protected boolean removeEldestEntry(Map.Entry<String, Boolean> eldest) { // 删除最早的
            return this.size() > 1000;
        }
    };

    /**
     * 检测会员Id是否合法
     */
    public static boolean checkMemberId(String memberId) {
        String regex = "^[0-9a-zA-Z].*$";
        if (StringUtils.isNotEmpty(memberId)) {
            return memberId.trim().matches(regex);
        }
        return false;
    }

    /**
     * 判断是否为新增会员
     */
    public static boolean isNewMember(String memberId, Connection conn, Configuration conf) {
        PreparedStatement ps = null;
        ResultSet rs = null;
        Boolean res = false;
        try {
            // 先检查缓存中是否存在
            if (!cache.containsKey(memberId)) {
                String sql = conf.get(GlobalConstants.PREFIX_TOTAL + KpiType.EMPTY_MEMBER.kpiName);
                ps = conn.prepareStatement(sql);
                ps.setString(1, memberId);
                rs = ps.executeQuery();
                if (rs.next()) { // 不是新增
                    res = false;
                } else { // 新增
                    res = true;
                }
                // 添加到cache中
                cache.put(memberId, res);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return res == null ? false : res.booleanValue();
    }
}