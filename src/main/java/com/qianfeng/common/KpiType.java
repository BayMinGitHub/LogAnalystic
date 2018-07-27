package com.qianfeng.common;

/**
 * @Description: kpi的枚举
 * Author by BayMin, Date on 2018/7/27.
 */
public enum KpiType {
    NEW_USER("new_user"),
    BROWSER_NEW_INSTALL_USER("browser_new_install_user"),
    ACTIVE_USER("active_user"),
    BROWSER_ACTIVE_USER("browser_active_user"),
    ACTIVE_MEMBER("active_member"),
    BROWSER_ACTIVE_MEMBER("browser_active_member"),
    NEW_MEMBER("new_member"),
    BROWSER_NEW_MEMBER("browser_new_member"),
    MEMBER_INFO("member_info"),
    SESSION("session"),
    BROWSER_SESSION("browser_session"),
    HOURLY_ACTIVE_USER("hourly_active_user"),
    HOURLY_SESSION("hourly_session"),
    PAGE_VIEW("pageview"),
    LOCATION("location"),;

    public final String kpiName;

    KpiType(String kpiName) {
        this.kpiName = kpiName;
    }

    /**
     * 根据kpi的name获取kpi的枚举
     */
    public KpiType valueOfType(String type) {
        for (KpiType kpi : values()) {
            if (type.equals(kpi.kpiName))
                return kpi;
        }
        throw new RuntimeException("暂不支持该类型获取时间枚举" + type);
    }
}
