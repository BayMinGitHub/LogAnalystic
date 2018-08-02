package com.bay.analystic.mr.pv;

import com.bay.analystic.model.dim.base.BaseDimension;
import com.bay.analystic.model.dim.key.StatsUserDimension;
import com.bay.analystic.model.dim.out.OutputWritter;
import com.bay.analystic.model.dim.value.reduce.MapWritableValue;
import com.bay.analystic.model.dim.value.OutputValueBaseWritable;
import com.bay.analystic.service.IDimensionConvert;
import com.bay.common.GlobalConstants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * @Description: 为新增用户的ps赋值
 * Author by BayMin, Date on 2018/7/30.
 */
public class PageViewOutputWritter implements OutputWritter {
    @Override
    public void outputWrite(Configuration conf, BaseDimension key, OutputValueBaseWritable value, PreparedStatement ps, IDimensionConvert convert) throws IOException, SQLException {
        StatsUserDimension statsUserDimension = (StatsUserDimension) key;
        MapWritableValue v = (MapWritableValue) value;
        int pv = ((IntWritable) ((MapWritableValue) value).getValue().get(new IntWritable(-1))).get();
        //为ps赋值
        int i = 0;
        ps.setInt(++i, convert.getDimensionIDByDimension(statsUserDimension.getStatsCommonDimension().getDateDimension()));
        ps.setInt(++i, convert.getDimensionIDByDimension(statsUserDimension.getStatsCommonDimension().getPlatFormDimension()));
        ps.setInt(++i, convert.getDimensionIDByDimension(statsUserDimension.getBrowserDimension()));
        ps.setInt(++i, pv);
        ps.setString(++i, conf.get(GlobalConstants.RUNNING_DATE));
        ps.setInt(++i, pv);
        //添加到批处理中
        ps.addBatch();
    }
}