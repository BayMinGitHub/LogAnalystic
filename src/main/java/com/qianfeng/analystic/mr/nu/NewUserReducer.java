package com.qianfeng.analystic.mr.nu;

import com.qianfeng.analystic.model.dim.base.PlatFormDimension;
import com.qianfeng.analystic.model.dim.key.StatsUserDimension;
import com.qianfeng.analystic.model.dim.value.map.TimeOutputValue;
import com.qianfeng.analystic.model.dim.value.reduce.MapWritableValue;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * @Description: 新增的用户和新增的总用户统计的Reducer类
 * Author by BayMin, Date on 2018/7/27.
 */
public class NewUserReducer extends Reducer<StatsUserDimension, TimeOutputValue, StatsUserDimension, MapWritableValue> {
    PlatFormDimension platFormDimension = new PlatFormDimension("website");
}
