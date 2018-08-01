package com.bay.analystic.mr.au;

import com.bay.analystic.model.dim.key.StatsUserDimension;
import com.bay.analystic.model.dim.value.TimeOutputValue;
import com.bay.analystic.model.dim.value.MapWritableValue;
import com.bay.common.DateEnum;
import com.bay.common.KpiType;
import com.bay.util.TimeUtil;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;

/**
 * @Description: 活跃用户的Reducer类
 * Author by BayMin, Date on 2018/7/27.
 */
public class ActiveUserReducer extends Reducer<StatsUserDimension, TimeOutputValue, StatsUserDimension, MapWritableValue> {
    // 用来去重
    private Set<String> unique = new HashSet<>();
    private Map<Integer, Integer> map = new HashMap<>();
    private MapWritableValue v = new MapWritableValue();

    @Override
    protected void reduce(StatsUserDimension key, Iterable<TimeOutputValue> values, Context context) throws IOException, InterruptedException {
        MapWritable mapWritable = new MapWritable();
        // 清空unique
        this.unique.clear();
        // 循环给map赋值
        for (int i = 1; i < 25; i++) {
            map.put(i, 0);
        }
        // 循环map阶段传过来的value
        for (TimeOutputValue tv : values) {
            // 将uuid取出来添加到set中
            this.unique.add(tv.getId());
            // 将时间取出
            long serverTime = tv.getTime();
            int hour = TimeUtil.getDateInfo(serverTime, DateEnum.HOUR) + 1;
            int count = map.get(hour);
            count++;
            map.put(hour, count);
        }
        this.v.setKpi(KpiType.HOURLY_ACTIVE_USER);
        for (Map.Entry<Integer, Integer> en : map.entrySet()) {
            mapWritable.put(new IntWritable(-en.getKey()), new IntWritable(en.getValue()));
        }
        this.v.setValue(mapWritable);
        context.write(key, this.v);
        // 构建输出的value
        mapWritable.put(new IntWritable(-1), new IntWritable(this.unique.size()));
        // 设置kpi
        this.v.setValue(mapWritable);
        this.v.setKpi(KpiType.valueOfType(key.getStatsCommonDimension().getKpiDimension().getKpiName()));
        // 输出
        context.write(key, this.v);
    }
}
