package com.bay.analystic.hive.udf;

import com.bay.analystic.model.dim.base.DateDimension;
import com.bay.analystic.service.IDimensionConvert;
import com.bay.analystic.service.impl.IDimensionConvertImpl;
import com.bay.common.DateEnum;
import com.bay.util.TimeUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

/**
 * @Description: 时间维度UDF
 * Author by BayMin, Date on 2018/8/2.
 */
public class DateDimensionUDF extends UDF {
    private IDimensionConvert convert = new IDimensionConvertImpl();

    /**
     * 获取id
     */
    public int evaluate(String time) {
        if (StringUtils.isEmpty(time))
            time = TimeUtil.getYesterdayDate();
        DateDimension dateDimension = DateDimension.buildDate(TimeUtil.parserString2Long(time), DateEnum.DAY);
        try {
            return convert.getDimensionIDByDimension(dateDimension);
        } catch (Exception e) {
            throw new RuntimeException("获取时间维度的UDF异常", e);
        }
    }

    public IntWritable evaluate(Text time) {
        if (StringUtils.isEmpty(time.toString()))
            time = new Text(TimeUtil.getYesterdayDate());
        DateDimension dateDimension = DateDimension.buildDate(TimeUtil.parserString2Long(time.toString()), DateEnum.DAY);
        try {
            return new IntWritable(convert.getDimensionIDByDimension(dateDimension));
        } catch (Exception e) {
            throw new RuntimeException("获取时间维度的UDF异常", e);
        }
    }
}
