package com.qianfeng.analystic.mr.nm;

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
import com.qianfeng.util.JDBCUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

/**
 * @Description: 活跃会员
 * Author by BayMin, Date on 2018/7/31.
 */
public class NewMemberMapper extends TableMapper<StatsUserDimension, TimeOutputValue> {

    private static final Logger logger = Logger.getLogger(com.qianfeng.analystic.mr.nu.NewUserMapper.class);
    private byte[] family = Bytes.toBytes(EventLogConstants.HBASE_COLUMN_FAMILY);
    private StatsUserDimension k = new StatsUserDimension();
    private TimeOutputValue v = new TimeOutputValue();
    private KpiDimension newMember = new KpiDimension(KpiType.NEW_MEMBER.kpiName);
    private KpiDimension browserNewMemberKpi = new KpiDimension(KpiType.BROWSER_NEW_MEMBER.kpiName);
    private Connection conn = null;
    private PreparedStatement ps = null;
    private ResultSet rs = null;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        try {
            conn = JDBCUtil.getConn();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
        // 获取需要的字段
        String memberId = Bytes.toString(value.getValue(family, Bytes.toBytes(EventLogConstants.EVENT_COLUMN_NAME_MEMBER_ID)));
        String serverTime = Bytes.toString(value.getValue(family, Bytes.toBytes(EventLogConstants.EVENT_COLUMN_NAME_SERVER_TIME)));
        String platform = Bytes.toString(value.getValue(family, Bytes.toBytes(EventLogConstants.EVENT_COLUMN_NAME_PLATFORM)));
        String browserName = Bytes.toString(value.getValue(family, Bytes.toBytes(EventLogConstants.EVENT_COLUME_NAME_BROWSER_NAME)));
        String browserVersion = Bytes.toString(value.getValue(family, Bytes.toBytes(EventLogConstants.EVENT_COLUME_NAME_BROWSER_VERSION)));

        // 对三个字段进行空判断
        if (StringUtils.isEmpty(memberId) || StringUtils.isEmpty(serverTime) || StringUtils.isEmpty(platform)) {
            logger.warn("uuid,serverTime,platform中有空值" + "memberId = " + memberId + "serverTime" + serverTime + "platform" + platform);
            return;
        }
        ps = null;
        rs = null;
        // 判断memberId是否存在
        try {
            ps = conn.prepareStatement("select `member_id` from `member_info` where `member_id` = ?");
            ps.setString(1, memberId);
            rs = ps.executeQuery();
            if (rs.next()) {
                logger.warn("该会员不是新增会员:" + memberId);
                return;
            } else {
                logger.warn("该会员是新增会员:" + memberId);
                // 构建输出的value
                long serverTimeOfLong = Long.valueOf(serverTime);
                this.v.setId(memberId);
                this.v.setTime(serverTimeOfLong);

                // 构建输出的key
                List<PlatFormDimension> platFormDimensions = PlatFormDimension.buildList(platform);
                DateDimension dateDimension = DateDimension.buildDate(serverTimeOfLong, DateEnum.DAY);
                List<BrowserDimension> browserDimensionList = BrowserDimension.buildList(browserName, browserVersion);
                StatsCommonDimension statsCommonDimension = this.k.getStatsCommonDimension();

                BrowserDimension defaultBrowserDimension = new BrowserDimension("", "");
                // 为statsCommonDimension赋值
                statsCommonDimension.setDateDimension(dateDimension);
                // 循环平台维度集合对象
                for (PlatFormDimension pl : platFormDimensions) {
                    statsCommonDimension.setKpiDimension(newMember);
                    statsCommonDimension.setPlatFormDimension(pl);
                    this.k.setStatsCommonDimension(statsCommonDimension);
                    this.k.setBrowserDimesion(defaultBrowserDimension);
                    // 输出
                    context.write(this.k, this.v);
                    for (BrowserDimension dimension : browserDimensionList) {
                        statsCommonDimension.setKpiDimension(browserNewMemberKpi);
                        this.k.setStatsCommonDimension(statsCommonDimension);
                        this.k.setBrowserDimesion(dimension);
                        context.write(this.k, this.v);
                    }
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        JDBCUtil.close(conn, ps, rs);
    }
}
