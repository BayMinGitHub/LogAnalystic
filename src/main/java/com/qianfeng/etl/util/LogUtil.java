package com.qianfeng.etl.util;

import com.qianfeng.common.EventLogConstants;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @Description: 日志工具类
 * Author by BayMin, Date on 2018/7/26.
 */
public class LogUtil {
    private static final Logger logger = Logger.getLogger(LogUtil.class);

    /**
     * 192.168.216.1^A
     * 1532606852.904^A
     * hadoop010^A
     * /index.html?ver=1.0&u_mid=test&en=e_cs&c_time=1532605298840&sdk=java_sdk&oid=123&pl=java_server&b_iev=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0.3396.99 Safari/537.36
     * 192.168.216.111^A1532576375.965^A192.168.216.111^A/index.html?ver=1.0&u_mid=123&en=e_cr&c_time=1532576375614&
     * ip:192.168.216.120
     * s_time:1532576375614
     * ver:1.0
     */
    public Map<String, String> parserLog(String log) {
        // 定义一个map
        Map<String, String> info = new ConcurrentHashMap<>();
        if (StringUtils.isNotEmpty(log)) {
            String[] fields = log.split(EventLogConstants.COLUMN_SEPARTOR);
            if (fields.length == 4) {
                // 向info赋值
                info.put(EventLogConstants.EVENT_COLUMN_NAME_IP, fields[0]);
                info.put(EventLogConstants.EVENT_COLUMN_NAME_SERVER_TIME, fields[1].replaceAll("\\.", ""));
                int index = fields[3].indexOf("?");
                if (index > 0) {
                    // 获取参数列表
                    String params = fields[3].substring(index + 1);
                    handleParams(info, params);
                    // 解析IP
                    handleIp(info);
                    // 解析userAgent
                    handleUserAgent(info);
                }
                return info;
            }
        }
        return null;
    }


    /**
     * 将参数列表的K-V参数存储到info中
     */
    private void handleParams(Map<String, String> info, String params) {
        if (StringUtils.isNotEmpty(params)) {
            String[] paramkvs = params.split("&");
            try {
                for (String paramkv : paramkvs) {
                    String kvs[] = paramkv.split("=");
                    String k = kvs[0];
                    String v = URLDecoder.decode(kvs[1], "utf-8");
                    if (StringUtils.isNotEmpty(k))
                        info.put(k, v);
                }
            } catch (UnsupportedEncodingException e) {
                logger.warn("处理参数列表异常", e);
            }
        }
    }

    /**
     * 解析IP,调之前的类
     */
    private void handleIp(Map<String, String> info) {
        if (info.containsKey(EventLogConstants.EVENT_COLUMN_NAME_IP)) {
            IpParserUtil.RegionInfo region = new IpParserUtil().parserIp(info.get(EventLogConstants.EVENT_COLUMN_NAME_IP));
            if (region != null) {
                info.put(EventLogConstants.EVENT_COLUMN_NAME_COUNTRY, region.getCountry());
                info.put(EventLogConstants.EVENT_COLUMN_NAME_PROVINCE, region.getProvince());
                info.put(EventLogConstants.EVENT_COLUMN_NAME_CITY, region.getCity());
            }
        }
    }

    /**
     * 解析userAgent
     */
    private void handleUserAgent(Map<String, String> info) {
        if (info.containsKey(EventLogConstants.EVENT_COLUMN_NAME_USERAGENT)) {
            UserAgentUtil.UserAgentInfo userAgentInfo = new UserAgentUtil().parserUserAgent(info.get(EventLogConstants.EVENT_COLUMN_NAME_USERAGENT));
            if (userAgentInfo != null) {
                info.put(EventLogConstants.EVENT_COLUMN_NAME_BROWSER_NAME, userAgentInfo.getBrowserName());
                info.put(EventLogConstants.EVENT_COLUMN_NAME_BROWSER_VERSION, userAgentInfo.getBrowserVersion());
                info.put(EventLogConstants.EVENT_COLUMN_NAME_OS_NAME, userAgentInfo.getOsName());
                info.put(EventLogConstants.EVENT_COLUMN_NAME_OS_VERSION, userAgentInfo.getOsVersion());
//                if (userAgentInfo.getBrowserName() != null)
//                    info.put(EventLogConstants.EVENT_COLUMN_NAME_BROWSER_NAME, userAgentInfo.getBrowserName());
//                else
//                    info.put(EventLogConstants.EVENT_COLUMN_NAME_BROWSER_NAME, "unknown");
//
//                if (userAgentInfo.getBrowserVersion() != null)
//                    info.put(EventLogConstants.EVENT_COLUMN_NAME_BROWSER_VERSION, userAgentInfo.getBrowserVersion());
//                else
//                    info.put(EventLogConstants.EVENT_COLUMN_NAME_BROWSER_VERSION, "unknown");
//
//                if (userAgentInfo.getOsName() != null)
//                    info.put(EventLogConstants.EVENT_COLUMN_NAME_OS_NAME, userAgentInfo.getOsName());
//                else
//                    info.put(EventLogConstants.EVENT_COLUMN_NAME_OS_NAME, "unknown");
//
//                if (userAgentInfo.getOsVersion() != null)
//                    info.put(EventLogConstants.EVENT_COLUMN_NAME_OS_VERSION, userAgentInfo.getOsVersion());
//                else
//                    info.put(EventLogConstants.EVENT_COLUMN_NAME_OS_VERSION, "unknown");
            }
        }
    }

}
