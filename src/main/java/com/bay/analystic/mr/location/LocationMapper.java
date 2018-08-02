package com.bay.analystic.mr.location;

import com.bay.analystic.model.dim.base.*;
import com.bay.analystic.model.dim.key.StatsCommonDimension;
import com.bay.analystic.model.dim.key.StatsLocationDimension;
import com.bay.analystic.model.dim.value.TextOutputValue;
import com.bay.common.DateEnum;
import com.bay.common.EventLogConstants;
import com.bay.common.KpiType;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.List;

/**
 * @Description: Location的Mapper类
 * Author by BayMin, Date on 2018/7/27.
 */
public class LocationMapper extends TableMapper<StatsLocationDimension, TextOutputValue> {
    private static final Logger logger = Logger.getLogger(LocationMapper.class);
    private byte[] family = Bytes.toBytes(EventLogConstants.HBASE_COLUMN_FAMILY);
    private StatsLocationDimension k = new StatsLocationDimension();
    private TextOutputValue v = new TextOutputValue();
    private KpiDimension locationKpi = new KpiDimension(KpiType.LOCATION.kpiName);

    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
        // 获取需要的字段
        String uuid = Bytes.toString(value.getValue(family, Bytes.toBytes(EventLogConstants.EVENT_COLUMN_NAME_UUID)));
        String sessionId = Bytes.toString(value.getValue(family, Bytes.toBytes(EventLogConstants.EVENT_COLUMN_NAME_SESSION_ID)));
        String serverTime = Bytes.toString(value.getValue(family, Bytes.toBytes(EventLogConstants.EVENT_COLUMN_NAME_SERVER_TIME)));
        String platform = Bytes.toString(value.getValue(family, Bytes.toBytes(EventLogConstants.EVENT_COLUMN_NAME_PLATFORM)));
        String country = Bytes.toString(value.getValue(family, Bytes.toBytes(EventLogConstants.EVENT_COLUMN_NAME_COUNTRY)));
        String province = Bytes.toString(value.getValue(family, Bytes.toBytes(EventLogConstants.EVENT_COLUMN_NAME_PROVINCE)));
        String city = Bytes.toString(value.getValue(family, Bytes.toBytes(EventLogConstants.EVENT_COLUMN_NAME_CITY)));

        // 对字段进行空判断
        if (StringUtils.isEmpty(serverTime) || StringUtils.isEmpty(platform)) {
            logger.warn("serverTime,platform中有空值" + "serverTime" + serverTime + "platform" + platform);
            return;
        }
        if (uuid == null)
            uuid = "";
        if (sessionId == null)
            sessionId = "";

        // 构建输出的value
        long serverTimeOfLong = Long.valueOf(serverTime);
        this.v.setUuid(uuid);
        this.v.setSessionId(sessionId);

        // 构建输出的key
        List<PlatFormDimension> platFormDimensions = PlatFormDimension.buildList(platform);
        DateDimension dateDimension = DateDimension.buildDate(serverTimeOfLong, DateEnum.DAY);
        List<LocationDimension> locationDimensionList = LocationDimension.buildList(country, province, city);

        StatsCommonDimension statsCommonDimension = this.k.getStatsCommonDimension();
        // 为statsCommonDimension赋值
        statsCommonDimension.setDateDimension(dateDimension);

        // 循环平台维度集合对象
        for (PlatFormDimension pl : platFormDimensions) {
            statsCommonDimension.setKpiDimension(locationKpi);
            statsCommonDimension.setPlatFormDimension(pl);
            this.k.setStatsCommonDimension(statsCommonDimension);
            // 输出用于统计地域模块的
            for (LocationDimension locationDimension : locationDimensionList) {
                this.k.setLocationDimension(locationDimension);
                context.write(this.k, this.v);
            }
        }
    }
}
