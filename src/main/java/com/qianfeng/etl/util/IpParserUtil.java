package com.qianfeng.etl.util;

import com.alibaba.fastjson.JSONObject;
import com.qianfeng.etl.util.ip.IPSeeker;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

/**
 * @Description: IP解析工具类, 最终会调用父类IPSeeker
 * 如果IP是Null,则返回Null
 * 如果IP是国外的IP,则直接显示国家即可
 * 如果IP是国内的IP,则直接显示国家,省,市
 * Author by BayMin, Date on 2018/7/25.
 */
public class IpParserUtil extends IPSeeker {
    private static final Logger logger = Logger.getLogger(IpParserUtil.class);
    RegionInfo info = new RegionInfo(); // 默认

    /**
     * @return 使用IPSeeker
     */
    public RegionInfo parserIp(String ip) {
        // 判断IP是否为空
        if (StringUtils.isEmpty(ip) || StringUtils.isEmpty(ip.trim()))
            return null;
        try {
            //ip不为空，正常解析
            String country = super.getCountry(ip);
            if ("局域网".equals(country)) { // 配置局域网信息
                info.setCountry("中国");
                info.setProvince("北京");
                info.setCity("昌平区");
            } else if (country != null || StringUtils.isNotEmpty(ip.trim())) {
                //查找省的位置
                info.setCountry("中国");
                int index = country.indexOf("省");
                if (index > 0) {
                    //设置省份
                    info.setProvince(country.substring(0, index + 1));
                    //判断是否有市
                    int index2 = country.indexOf("市");
                    if (index2 > 0) {
                        //设置市
                        info.setCity(country.substring(index + 1, Math.min(index2 + 1, country.length())));
                    }
                } else {
                    //代码走到这儿，就代表没有省份.就是直辖市、自治区、特别行政区
                    String flag = country.substring(0, 2);
                    switch (flag) {
                        case "内蒙":
                            info.setProvince("内蒙古");
                            country = country.substring(3);
                            index = country.indexOf("市");
                            if (index > 0) {
                                info.setCity(country.substring(0,
                                        Math.min(index + 1, country.length())));
                            }
                            break;
                        case "广西":
                        case "西藏":
                        case "新疆":
                        case "宁夏":
                            info.setProvince(flag);
                            country = country.substring(2);
                            index = country.indexOf("市");
                            if (index > 0) {
                                info.setCity(country.substring(0,
                                        Math.min(index + 1, country.length())));
                            }
                            break;
                        case "北京":
                        case "上海":
                        case "重庆":
                        case "天津":
                            info.setProvince(flag + "市");
                            country = country.substring(3);
                            index = country.indexOf("区");
                            if (index > 0) {
                                char ch = country.charAt(index - 1);
                                if (ch != '小' && ch != '校' && ch != '军') {
                                    info.setCity(country.substring(0,
                                            Math.min(index + 1, country.length())));
                                }
                            }
                            //在直辖市中如果有县
                            index = country.indexOf("县");
                            if (index > 0) {
                                info.setCity(country.substring(0,
                                        Math.min(index + 1, country.length())));
                            }
                            break;
                        case "香港":
                        case "澳门":
                        case "台湾":
                            info.setProvince(flag + "特别行政区");
                            break;
                        default:
                            break;
                    }
                }
            }
            if (info.getProvince() == info.getDEFAULT_VALUE())
                info.setCountry(country);
        } catch (Exception e) {
            logger.warn("解析ip工具方法异常");
        }

        return info;
    }

    /**
     * 使用淘宝提供的查询IP的API接口
     */
    public RegionInfo parserIp1(String url, String charset) {
        HttpClient client = new HttpClient();
        GetMethod method = new GetMethod(url);

        try {
            if (null == url || !url.startsWith("http")) {
                throw new Exception("请求地址格式不对");
            }
            // 设置请求的编码格式
            if (null != charset) {
                method.addRequestHeader("Content-Type", "application/x-www-from-urlencoded; charset=" + charset);
            } else {
                method.addRequestHeader("Content-Type", "application/x-www-from-urlencoded; charset=" + "utf-8");
            }
            int statusCode = client.executeMethod(method);

            if (statusCode != HttpStatus.SC_OK) // 打印服务器返回的状态
                System.out.println("Method failed: " + method.getStatusLine());
            // 返回相应消息
            byte[] responseBody = method.getResponseBodyAsString().getBytes(method.getRequestCharSet());
            // 在返回相应消息时使用编码(utf-8或gb2312)
            String response = new String(responseBody, "utf-8");

            JSONObject jo = JSONObject.parseObject(response); // 获取JSON格式的信息
            JSONObject j = (JSONObject) jo.get("data");
            // 返回的JSON为
            // {"code":0,"data":{"ip":"59.67.194.5","country":"中国","area":"","region":"河北","city":"石家庄","county":"XX","isp":"教育网","country_id":"CN","area_id":"","region_id":"130000","city_id":"130100","county_id":"xx","isp_id":"100027"}}

            // 释放连接
            method.releaseConnection();
            // 设置省市
            info.setCountry(j.get("country").toString());
            info.setProvince(j.get("region").toString());
            info.setCity(j.get("city").toString());
        } catch (Exception e) {
            e.printStackTrace();
        }
        return info;

    }

    /**
     * 使用该类来进行封装地域信息,IP解析出来的国家,省,市
     */
    public static class RegionInfo {
        private final String DEFAULT_VALUE = "unknown";
        private String country = DEFAULT_VALUE;
        private String province = DEFAULT_VALUE;
        private String city = DEFAULT_VALUE;

        @Override
        public String toString() {
            return "RegionInfo{country='" + country + ", province='" + province + ", city='" + city + '}';
        }

        public String getDEFAULT_VALUE() {
            return DEFAULT_VALUE;
        }

        public String getCountry() {
            return country;
        }

        public void setCountry(String country) {
            this.country = country;
        }

        public String getProvince() {
            return province;
        }

        public void setProvince(String province) {
            this.province = province;
        }

        public String getCity() {
            return city;
        }

        public void setCity(String city) {
            this.city = city;
        }
    }
}
