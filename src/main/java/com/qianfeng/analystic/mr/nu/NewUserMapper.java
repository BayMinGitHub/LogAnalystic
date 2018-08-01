package com.qianfeng.analystic.mr.nu;

import com.qianfeng.analystic.model.dim.base.BrowserDimension;
import com.qianfeng.analystic.model.dim.base.DateDimension;
import com.qianfeng.analystic.model.dim.base.KpiDimension;
import com.qianfeng.analystic.model.dim.base.PlatFormDimension;
import com.qianfeng.analystic.model.dim.key.StatsCommonDimension;
import com.qianfeng.analystic.model.dim.key.StatsUserDimension;
import com.qianfeng.analystic.model.dim.value.TimeOutputValue;
import com.qianfeng.common.DateEnum;
import com.qianfeng.common.EventLogConstants;
import com.qianfeng.common.KpiType;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.List;

/**
 * @Description: 新增的用户和新增的总用户统计的Mapper类, 需要Launch时间中的uuid为一个数
 * Author by BayMin, Date on 2018/7/27.
 */
public class NewUserMapper extends TableMapper<StatsUserDimension, TimeOutputValue> {
    private static final Logger logger = Logger.getLogger(NewUserMapper.class);
    private byte[] family = Bytes.toBytes(EventLogConstants.HBASE_COLUMN_FAMILY);
    private StatsUserDimension k = new StatsUserDimension();
    private TimeOutputValue v = new TimeOutputValue();
    private KpiDimension newUserKpi = new KpiDimension(KpiType.NEW_USER.kpiName);
    private KpiDimension browserNewUserKpi = new KpiDimension(KpiType.BROWSER_NEW_USER.kpiName);

    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
        // 获取需要的字段
        // TODO 老师讲BUG时认真听讲
        String uuid = Bytes.toString(value.getValue(family, Bytes.toBytes(EventLogConstants.EVENT_COLUMN_NAME_UUID)));
        String serverTime = Bytes.toString(value.getValue(family, Bytes.toBytes(EventLogConstants.EVENT_COLUMN_NAME_SERVER_TIME)));
        String platform = Bytes.toString(value.getValue(family, Bytes.toBytes(EventLogConstants.EVENT_COLUMN_NAME_PLATFORM)));
        String browserName = Bytes.toString(value.getValue(family, Bytes.toBytes(EventLogConstants.EVENT_COLUMN_NAME_BROWSER_NAME)));
        String browserVersion = Bytes.toString(value.getValue(family, Bytes.toBytes(EventLogConstants.EVENT_COLUMN_NAME_BROWSER_VERSION)));

        // 对三个字段进行空判断
        if (StringUtils.isEmpty(uuid) || StringUtils.isEmpty(serverTime) || StringUtils.isEmpty(platform)) {
            logger.warn("uuid,serverTime,platform中有空值" + "uuid = " + uuid + "serverTime" + serverTime + "platform" + platform);
            return;
        }

        // 构建输出的value
        long serverTimeOfLong = Long.valueOf(serverTime);
        this.v.setId(uuid);
        this.v.setTime(serverTimeOfLong);

        // 构建输出的key
        List<PlatFormDimension> platFormDimensions = PlatFormDimension.buildList(platform);
        DateDimension dateDimension = DateDimension.buildDate(serverTimeOfLong, DateEnum.DAY);
        List<BrowserDimension> browserDimensionList = BrowserDimension.buildList(browserName, browserVersion);

        StatsCommonDimension statsCommonDimension = this.k.getStatsCommonDimension();
        // 为statsCommonDimension赋值
        statsCommonDimension.setDateDimension(dateDimension);

        BrowserDimension defaultBrowserDimension = new BrowserDimension("", "");
        // 循环平台维度集合对象
        for (PlatFormDimension pl : platFormDimensions) {
            statsCommonDimension.setKpiDimension(newUserKpi);
            statsCommonDimension.setPlatFormDimension(pl);
            this.k.setStatsCommonDimension(statsCommonDimension);
            this.k.setBrowserDimesion(defaultBrowserDimension);
            // 输出
            context.write(this.k, this.v);

            for (BrowserDimension dimension : browserDimensionList) {
                statsCommonDimension.setKpiDimension(browserNewUserKpi);
                this.k.setStatsCommonDimension(statsCommonDimension);
                this.k.setBrowserDimesion(dimension);
                context.write(this.k, this.v);
            }
        }
    }
}
