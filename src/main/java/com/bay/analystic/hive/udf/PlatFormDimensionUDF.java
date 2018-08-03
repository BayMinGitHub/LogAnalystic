package com.bay.analystic.hive.udf;

import com.bay.analystic.model.dim.base.PlatFormDimension;
import com.bay.analystic.service.IDimensionConvert;
import com.bay.analystic.service.impl.IDimensionConvertImpl;
import com.bay.common.GlobalConstants;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

/**
 * @Description: 平台维度UDF
 * Author by BayMin, Date on 2018/8/2.
 */
public class PlatFormDimensionUDF extends UDF {
    private IDimensionConvert convert = new IDimensionConvertImpl();

    public int evaluate(String platformName) {
        if (StringUtils.isEmpty(platformName))
            platformName = GlobalConstants.DEFAULT_VALUE;
        PlatFormDimension platFormDimension = new PlatFormDimension(platformName);
        try {
            return convert.getDimensionIDByDimension(platFormDimension);
        } catch (Exception e) {
            throw new RuntimeException("执行平台维度UDF时异常", e);
        }
    }

    public IntWritable evaluate(Text platformName) {
        if (StringUtils.isEmpty(platformName.toString()))
            platformName = new Text(GlobalConstants.DEFAULT_VALUE);
        PlatFormDimension platFormDimension = new PlatFormDimension(platformName.toString());
        try {
            return new IntWritable(convert.getDimensionIDByDimension(platFormDimension));
        } catch (Exception e) {
            throw new RuntimeException("执行平台维度UDF时异常", e);
        }
    }
}
