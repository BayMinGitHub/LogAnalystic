package com.qianfeng.analystic.mr.nu;

import com.qianfeng.analystic.model.dim.base.BaseDimension;
import com.qianfeng.analystic.model.dim.key.StatsUserDimension;
import com.qianfeng.analystic.model.dim.value.OutputValueBaseWritable;
import com.qianfeng.analystic.model.dim.value.reduce.MapWritableValue;
import com.qianfeng.analystic.mr.out.OutputWritter;
import com.qianfeng.analystic.service.IDimensionConvert;

import com.qianfeng.common.GlobalConstants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * @Description: 为新增用户的ps赋值
 * Author by BayMin, Date on 2018/7/30.
 */
public class NewUserOutputWritter implements OutputWritter {
    @Override
    public void outputWrite(Configuration conf, BaseDimension key, OutputValueBaseWritable value, PreparedStatement ps, IDimensionConvert convert) throws IOException, SQLException {
        StatsUserDimension statsUserDimension = (StatsUserDimension) key;
        int newUsers = ((IntWritable) ((MapWritableValue) value).getValue().get(new IntWritable(-1))).get();
        //为ps赋值
        int i = 0;
        ps.setInt(++i, convert.getDimensionIDByDimension(statsUserDimension.getStatsCommonDimension().getDateDimension()));
        ps.setInt(++i, convert.getDimensionIDByDimension(statsUserDimension.getStatsCommonDimension().getPlatFormDimension()));
        ps.setInt(++i, newUsers);
        ps.setString(++i, conf.get(GlobalConstants.RUNNING_DATE));
        ps.setInt(++i, newUsers);
        //添加到批处理中
        ps.addBatch();
    }
}