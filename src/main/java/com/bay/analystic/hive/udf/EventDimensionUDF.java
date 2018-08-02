package com.bay.analystic.hive.udf;

import com.bay.analystic.model.dim.base.EventDimension;
import com.bay.analystic.service.IDimensionConvert;
import com.bay.analystic.service.impl.IDimensionConvertImpl;
import com.bay.common.GlobalConstants;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * @Description: 事件维度UDF
 * Author by BayMin, Date on 2018/8/2.
 */
public class EventDimensionUDF extends UDF {
    private IDimensionConvert convert = new IDimensionConvertImpl();

    /**
     * 获取id
     */
    public int evaluate(String category, String action) {
        if (StringUtils.isEmpty(category))
            category = GlobalConstants.DEFAULT_VALUE;
        if (StringUtils.isEmpty(action))
            action = GlobalConstants.DEFAULT_VALUE;
        EventDimension eventDimension = new EventDimension(category, action);
        try {
            return convert.getDimensionIDByDimension(eventDimension);
        } catch (Exception e) {
            throw new RuntimeException("获取事件维度的UDF异常", e);
        }
    }
}
