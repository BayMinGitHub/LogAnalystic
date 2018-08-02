package com.bay.analystic.mr.location;

import com.bay.analystic.model.dim.key.StatsLocationDimension;
import com.bay.analystic.model.dim.value.reduce.LocationReducerOutputWritable;
import com.bay.analystic.model.dim.value.map.TextOutputValue;
import com.bay.common.KpiType;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * @Description: 新增的用户和新增的总用户统计的Reducer类
 * Author by BayMin, Date on 2018/7/27.
 */
public class LocationReducer extends Reducer<StatsLocationDimension, TextOutputValue, StatsLocationDimension, LocationReducerOutputWritable> {
    // 用来去重uuid
    private Set<String> unique = new HashSet<>();
    // 用来统计session信息
    private Map<String, Integer> sessions = new HashMap<>();
    private LocationReducerOutputWritable v = new LocationReducerOutputWritable();

    @Override
    protected void reduce(StatsLocationDimension key, Iterable<TextOutputValue> values, Context context) throws IOException, InterruptedException {
        // 清空
        this.unique.clear();
        this.sessions.clear();
        // 循环map阶段传过来的value
        for (TextOutputValue tv : values) {
            // 将uuid取出来添加到set中
            this.unique.add(tv.getUuid());
            String sessionId = tv.getItem();
            if (sessions.containsKey(sessionId))
                this.sessions.put(sessionId, 2); // 只点击一次的称作跳出数
            else
                this.sessions.put(sessionId, 1);
        }
        // 构建输出的value
        this.v.setActiveUsers(unique.size());
        this.v.setSessions(this.sessions.size());
        int bounceNum = 0;
        for (Map.Entry<String, Integer> en : sessions.entrySet()) {
            if (en.getValue() == 1)
                bounceNum++;
        }
        this.v.setBounceSessions(bounceNum);
        this.v.setKpi(KpiType.valueOfType(key.getStatsCommonDimension().getKpiDimension().getKpiName()));
        // 输出
        context.write(key, this.v);
    }
}
