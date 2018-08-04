package com.bay.analystic.hive.udf;

import com.bay.analystic.model.dim.base.PaymentTypeDimension;
import com.bay.analystic.service.IDimensionConvert;
import com.bay.analystic.service.impl.IDimensionConvertImpl;
import com.bay.common.GlobalConstants;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

/**
 * @Description: 支付方式UDF
 * Author by BayMin, Date on 2018/8/4.
 */
public class PaymentTypeDimensionUDF extends UDF {
    private IDimensionConvert convert = new IDimensionConvertImpl();

    /**
     * 查ID
     */
    public int evaluate(String paymentType) {
        if (StringUtils.isEmpty(paymentType))
            paymentType = GlobalConstants.DEFAULT_VALUE;
        PaymentTypeDimension paymentTypeDimension = new PaymentTypeDimension(paymentType);
        try {
            return convert.getDimensionIDByDimension(paymentTypeDimension);
        } catch (Exception e) {
            throw new RuntimeException("获取支付类型维度的UDF异常", e);
        }
    }

    public IntWritable evaluate(Text paymentType) {
        if (StringUtils.isEmpty(paymentType.toString()))
            paymentType = new Text(GlobalConstants.DEFAULT_VALUE);
        PaymentTypeDimension paymentTypeDimension = new PaymentTypeDimension(paymentType.toString());
        try {
            return new IntWritable(convert.getDimensionIDByDimension(paymentTypeDimension));
        } catch (Exception e) {
            throw new RuntimeException("获取支付类型维度的UDF异常", e);
        }
    }
}
