package com.bay.analystic.hive.udf;

import com.bay.analystic.model.dim.base.CurrencyTypeDimension;
import com.bay.analystic.service.IDimensionConvert;
import com.bay.analystic.service.impl.IDimensionConvertImpl;
import com.bay.common.GlobalConstants;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

/**
 * @Description: 货币类型UDF
 * Author by BayMin, Date on 2018/8/4.
 */
public class CurrencyTypeDimensionUDF extends UDF {
    private IDimensionConvert convert = new IDimensionConvertImpl();

    /**
     * 查询ID
     */
    public int evaluate(String currencyName) {
        if (StringUtils.isEmpty(currencyName))
            currencyName = GlobalConstants.DEFAULT_VALUE;
        CurrencyTypeDimension currencyTypeDimension = new CurrencyTypeDimension(currencyName);
        try {
            return convert.getDimensionIDByDimension(currencyTypeDimension);
        } catch (Exception e) {
            throw new RuntimeException("获取货币维度的UDF异常", e);
        }
    }

    public IntWritable evaluate(Text currencyName) {
        if (StringUtils.isEmpty(currencyName.toString()))
            currencyName = new Text(GlobalConstants.DEFAULT_VALUE);
        CurrencyTypeDimension currencyTypeDimension = new CurrencyTypeDimension(currencyName.toString());
        try {
            return new IntWritable(convert.getDimensionIDByDimension(currencyTypeDimension));
        } catch (Exception e) {
            throw new RuntimeException("获取货币维度的UDF异常", e);
        }
    }
}
