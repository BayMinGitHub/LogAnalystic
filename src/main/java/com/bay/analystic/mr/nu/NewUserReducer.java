package com.bay.analystic.mr.nu;

import com.bay.analystic.model.dim.key.StatsUserDimension;
import com.bay.analystic.model.dim.value.map.TimeOutputValue;
import com.bay.analystic.model.dim.value.reduce.MapWritableValue;
import com.bay.common.KpiType;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * @Description: 新增的用户和新增的总用户统计的Reducer类
 * Author by BayMin, Date on 2018/7/27.
 */
public class NewUserReducer extends Reducer<StatsUserDimension, TimeOutputValue, StatsUserDimension, MapWritableValue> {
    // 用来去重
    private Set<String> unique = new HashSet<>();
    private MapWritableValue v = new MapWritableValue();

    @Override
    protected void reduce(StatsUserDimension key, Iterable<TimeOutputValue> values, Context context) throws IOException, InterruptedException {
        // 清空unique
        this.unique.clear();
        // 循环map阶段传过来的value
        for (TimeOutputValue tv : values) {
            // 将uuid取出来添加到set中
            this.unique.add(tv.getId());
        }
        // 构建输出的value
        MapWritable mapWritable = new MapWritable();
        mapWritable.put(new IntWritable(-1), new IntWritable(this.unique.size()));
        // 设置kpi
        this.v.setValue(mapWritable);
        this.v.setKpi(KpiType.valueOfType(key.getStatsCommonDimension().getKpiDimension().getKpiName()));
//        if (key.getStatsCommonDimension().getKpiDimension().getKpiName().equals(KpiType.NEW_USER.kpiName))
//            this.v.setKpi(KpiType.NEW_USER);
        // 输出
        context.write(key, this.v);
    }
}
