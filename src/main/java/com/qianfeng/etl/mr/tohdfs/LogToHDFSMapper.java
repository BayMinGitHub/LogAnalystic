package com.qianfeng.etl.mr.tohdfs;

import com.qianfeng.common.EventLogConstants;
import com.qianfeng.etl.mr.tohbase.LogToHbaseMapper;
import com.qianfeng.etl.util.LogUtil;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Map;


/**
 * @Description: 写入到HDFS的Mapper
 * Author by BayMin, Date on 2018/7/27.
 */
public class LogToHDFSMapper extends Mapper<LongWritable, Text, NullWritable, LogDataWritable> {
    private static final Logger logger = Logger.getLogger(LogToHbaseMapper.class);
    // 输入输出和过滤行记录
    private int inputRecords, outputRecords, filterRecords = 0;

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        inputRecords++;
        logger.info("输入的日志为: " + value.toString());
        try {
            Map<String, String> clientInfo = new LogUtil().parserLog(value.toString());
            //判断clientInfo是否为空
            if (clientInfo.isEmpty()) {
                filterRecords++;
                return;
            }
            //代码走到这儿，肯定有可用的key-value
            String eventName = clientInfo.get(EventLogConstants.EVENT_COLUMN_NAME_EVENT_NAME);
            EventLogConstants.EventEnum type = EventLogConstants.EventEnum.valueOfAlias(eventName);
            switch (type) {
                case LAUNCH:
                case PAGEVIEW:
                case CHARGESUCCESS:
                case CHARGEREFUND:
                case EVENT:
                case CHARGEREQUEST:
                    handleLogToHdfs(clientInfo, context);
                    break;
                default:
                    filterRecords++;
                    logger.warn("该事件不支持数据的清洗,事件类型为:" + eventName);
                    break;
            }
        } catch (Exception e) {
            logger.warn("数据清洗异常:" + e);
            filterRecords++;
        }
    }

    /**
     * 将clintInfo中的k-v输出到hdfs中
     */
    private void handleLogToHdfs(Map<String, String> clientInfo, Context context) {
        try {
            if (!clientInfo.isEmpty()) {
                LogDataWritable ld = new LogDataWritable();
                ld.s_time = clientInfo.get(EventLogConstants.EVENT_COLUMN_NAME_SERVER_TIME);
                ld.en = clientInfo.get(EventLogConstants.EVENT_COLUMN_NAME_EVENT_NAME);
                ld.ver = clientInfo.get(EventLogConstants.EVENT_COLUMN_NAME_VERSION);
                ld.u_ud = clientInfo.get(EventLogConstants.EVENT_COLUMN_NAME_UUID);
                ld.u_mid = clientInfo.get(EventLogConstants.EVENT_COLUMN_NAME_MEMBER_ID);
                ld.u_sid = clientInfo.get(EventLogConstants.EVENT_COLUMN_NAME_SESSION_ID);
                ld.c_time = clientInfo.get(EventLogConstants.EVENT_COLUMN_NAME_CLIENT_TIME);
                ld.language = clientInfo.get(EventLogConstants.EVENT_COLUMN_NAME_LANGUAGE);
                ld.b_iev = clientInfo.get(EventLogConstants.EVENT_COLUMN_NAME_USERAGENT);
                ld.b_rst = clientInfo.get(EventLogConstants.EVENT_COLUMN_NAME_RESOLUTION);
                ld.p_url = clientInfo.get(EventLogConstants.EVENT_COLUMN_NAME_CURRENT_URL);
                ld.p_ref = clientInfo.get(EventLogConstants.EVENT_COLUMN_NAME_PREFFER_URL);
                ld.tt = clientInfo.get(EventLogConstants.EVENT_COLUMN_NAME_TITLE);
                ld.pl = clientInfo.get(EventLogConstants.EVENT_COLUMN_NAME_PLATFORM);
                ld.oid = clientInfo.get(EventLogConstants.EVENT_COLUMN_NAME_ORDER_ID);
                ld.on = clientInfo.get(EventLogConstants.EVENT_COLUMN_NAME_ORDER_NAME);
                ld.cut = clientInfo.get(EventLogConstants.EVENT_COLUMN_NAME_CURRENCY_TYPE);
                ld.cua = clientInfo.get(EventLogConstants.EVENT_COLUMN_NAME_CURRENCY_AMOUTN);
                ld.pt = clientInfo.get(EventLogConstants.EVENT_COLUMN_NAME_PAYMENT_TYPE);
                ld.ca = clientInfo.get(EventLogConstants.EVENT_COLUMN_NAME_EVENT_NAME_CATEGORY);
                ld.ac = clientInfo.get(EventLogConstants.EVENT_COLUMN_NAME_EVENT_ACTION);
                ld.kv_ = clientInfo.get(EventLogConstants.EVENT_COLUMN_NAME_EVENT_KV);
                ld.du = clientInfo.get(EventLogConstants.EVENT_COLUMN_NAME_EVENT_DURATION);
                ld.os = clientInfo.get(EventLogConstants.EVENT_COLUMN_NAME_OS_NAME);
                ld.os_v = clientInfo.get(EventLogConstants.EVENT_COLUMN_NAME_OS_VERSION);
                ld.browser = clientInfo.get(EventLogConstants.EVENT_COLUMN_NAME_BROWSER_NAME);
                ld.browser_v = clientInfo.get(EventLogConstants.EVENT_COLUMN_NAME_BROWSER_NAME);
                ld.country = clientInfo.get(EventLogConstants.EVENT_COLUMN_NAME_COUNTRY);
                ld.province = clientInfo.get(EventLogConstants.EVENT_COLUMN_NAME_PROVINCE);
                ld.city = clientInfo.get(EventLogConstants.EVENT_COLUMN_NAME_CITY);
                //输出
                context.write(NullWritable.get(), ld);
                outputRecords++;
            }
        } catch (Exception e) {
            //e.printStackTrace();
            logger.warn("写数据到HDFS异常", e);
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        logger.info("total inputRecords:" + inputRecords + "  total outputRecords:" + outputRecords + "  total filterRecords:" + filterRecords);
    }
}

