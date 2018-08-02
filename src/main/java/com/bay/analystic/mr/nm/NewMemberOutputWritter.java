package com.bay.analystic.mr.nm;

import com.bay.analystic.model.dim.base.BaseDimension;
import com.bay.analystic.model.dim.key.StatsUserDimension;
import com.bay.analystic.model.dim.value.OutputValueBaseWritable;
import com.bay.analystic.model.dim.value.reduce.MapWritableValue;
import com.bay.analystic.model.dim.out.OutputWritter;
import com.bay.analystic.service.IDimensionConvert;
import com.bay.common.GlobalConstants;
import com.bay.common.KpiType;
import com.bay.util.TimeUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * @Description: 为会员的ps赋值
 * Author by BayMin, Date on 2018/7/30.
 */
public class NewMemberOutputWritter implements OutputWritter {
    @Override
    public void outputWrite(Configuration conf, BaseDimension key, OutputValueBaseWritable value, PreparedStatement ps, IDimensionConvert convert) throws IOException, SQLException {
        StatsUserDimension statsUserDimension = (StatsUserDimension) key;
        MapWritableValue v = (MapWritableValue) value;

        //为ps赋值
        int i = 0;
        switch (v.getKpi()) {
            case INSTALL_NEW_MEMBER:
                String memberId = ((MapWritableValue) value).getValue().get(new IntWritable(-2)).toString();
                ps.setString(++i, memberId);
                ps.setDate(++i, new Date(TimeUtil.parserString2Long(conf.get(GlobalConstants.RUNNING_DATE))));
                ps.setBigDecimal(++i, BigDecimal.valueOf(TimeUtil.parserString2Long(conf.get(GlobalConstants.RUNNING_DATE))));
                ps.setDate(++i, new Date(TimeUtil.parserString2Long(conf.get(GlobalConstants.RUNNING_DATE))));
                ps.setDate(++i, new Date(TimeUtil.parserString2Long(conf.get(GlobalConstants.RUNNING_DATE))));
                break;
            case NEW_MEMBER:
            case BROWSER_NEW_MEMBER:
                int newUsers = ((IntWritable) v.getValue().get(new IntWritable(-1))).get();
                ps.setInt(++i, convert.getDimensionIDByDimension(statsUserDimension.getStatsCommonDimension().getDateDimension()));
                ps.setInt(++i, convert.getDimensionIDByDimension(statsUserDimension.getStatsCommonDimension().getPlatFormDimension()));
                if (v.getKpi().kpiName.equals(KpiType.BROWSER_NEW_MEMBER.kpiName)) {
                    ps.setInt(++i, convert.getDimensionIDByDimension(statsUserDimension.getBrowserDimension()));
                }
                ps.setInt(++i, newUsers);
                ps.setString(++i, conf.get(GlobalConstants.RUNNING_DATE));
                ps.setInt(++i, newUsers);
                break;
            default:
                throw new RuntimeException("该kpi暂时不支持赋值:" + v.getKpi().kpiName);
        }
        //添加到批处理中
        ps.addBatch();
    }
}