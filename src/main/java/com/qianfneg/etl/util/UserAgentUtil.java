package com.qianfneg.etl.util;

import cz.mallat.uasparser.OnlineUpdater;
import cz.mallat.uasparser.UASparser;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * @Description: 浏览器代理对象的解析
 * Author by BayMin, Date on 2018/7/25.
 */
public class UserAgentUtil {
    private static final Logger logger = Logger.getLogger(UserAgentInfo.class);
    UserAgentInfo info = new UserAgentInfo();

    // 获取uasparser对象
    private static UASparser uaSparser = null;

    static {
        try {
            uaSparser = new UASparser(OnlineUpdater.getVendoredInputStream());
        } catch (IOException e) {
            logger.warn("获取uasparser对象失败.", e);
        }
    }

    /**
     * 解析浏览器的代理对象
     */
    public UserAgentInfo parserUserAgent(String userAgent) {
        if (StringUtils.isEmpty(userAgent))
            return null;
        // 使用uasparser获取对象代理信息
        try {
            cz.mallat.uasparser.UserAgentInfo ua = uaSparser.parse(userAgent); // 使用别人包中的
            if (ua != null) {
                // 为info设置信息
                info.setBrowserName(ua.getUaName());
                info.setBrowserVersion(ua.getBrowserVersionInfo());
                info.setOsName(ua.getOsName());
                info.setOsVersion(ua.getOsFamily());
            } else {
                info.setBrowserName("unknown");
                info.setBrowserVersion("unknown");
                info.setOsName("unknown");
                info.setOsVersion("unknown");
            }
        } catch (IOException e) {
            logger.warn("useragent解析异常");
        }
        return info;
    }

    /**
     * 封装浏览器相关信息
     */
    public static class UserAgentInfo {
        private String browserName;
        private String browserVersion;
        private String osName;
        private String osVersion;

        @Override
        public String toString() {
            return "UserAgentInfo{browserName=" + browserName + ", browserVersion='" + browserVersion + ", osName='" + osName + ", osVersion='" + osVersion + '}';
        }

        public String getBrowserName() {
            return browserName;
        }

        public void setBrowserName(String browserName) {
            this.browserName = browserName;
        }

        public String getBrowserVersion() {
            return browserVersion;
        }

        public void setBrowserVersion(String browserVersion) {
            this.browserVersion = browserVersion;
        }

        public String getOsName() {
            return osName;
        }

        public void setOsName(String osName) {
            this.osName = osName;
        }

        public String getOsVersion() {
            return osVersion;
        }

        public void setOsVersion(String osVersion) {
            this.osVersion = osVersion;
        }
    }
}
