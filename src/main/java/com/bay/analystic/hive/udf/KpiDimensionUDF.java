package com.bay.analystic.hive.udf;

import com.bay.analystic.model.dim.base.KpiDimension;
import com.bay.analystic.service.IDimensionConvert;
import com.bay.analystic.service.impl.IDimensionConvertImpl;
import com.bay.common.GlobalConstants;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

/**
 * @Description: Kpi维度UDF
 * Author by BayMin, Date on 2018/8/4.
 */
public class KpiDimensionUDF extends UDF {
    private IDimensionConvert convert = new IDimensionConvertImpl();

    /**
     * 查询id
     */
    public int evaluate(String kpiName) {
        if (StringUtils.isEmpty(kpiName))
            kpiName = GlobalConstants.DEFAULT_VALUE;
        KpiDimension kpiDimension = new KpiDimension(kpiName);
        try {
            return convert.getDimensionIDByDimension(kpiDimension);
        } catch (Exception e) {
            throw new RuntimeException("获取Kpi维度的UDF异常", e);
        }
    }

    public IntWritable evaluate(Text kpiName) {
        if (StringUtils.isEmpty(kpiName.toString()))
            kpiName = new Text(GlobalConstants.DEFAULT_VALUE);
        KpiDimension kpiDimension = new KpiDimension(kpiName.toString());
        try {
            return new IntWritable(convert.getDimensionIDByDimension(kpiDimension));
        } catch (Exception e) {
            throw new RuntimeException("获取Kpi维度的UDF异常", e);
        }
    }
}
