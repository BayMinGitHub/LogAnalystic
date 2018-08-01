package com.qianfeng.analystic.mr.session;

import com.qianfeng.analystic.model.dim.key.StatsUserDimension;
import com.qianfeng.analystic.model.dim.value.MapWritableValue;
import com.qianfeng.analystic.model.dim.value.TimeOutputValue;
import com.qianfeng.common.GlobalConstants;
import com.qianfeng.common.KpiType;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;

/**
 * @Description: 新增的用户和新增的总用户统计的Reducer类
 * Author by BayMin, Date on 2018/7/27.
 */
public class SessionReducer extends Reducer<StatsUserDimension, TimeOutputValue, StatsUserDimension, MapWritableValue> {
    // 用来去重
    private MapWritableValue v = new MapWritableValue();

    /**
     * 2018-07-26 website 111 123
     * 2018-07-26 website 111 125
     * 2018-07-26 website 112 133
     * <p>
     * 2018-07-26 website List((111,123),(111,123),(112,133))
     * <p>
     * 使用Map
     * 111-List(123,125,110,111)
     */
    private Map<String, List<Long>> map = new HashMap<>();

    @Override
    protected void reduce(StatsUserDimension key, Iterable<TimeOutputValue> values, Context context) throws IOException, InterruptedException {
        // 清空map
        this.map.clear();
        // 循环map阶段传过来的value
        for (TimeOutputValue tv : values) {
            // 将sessionId取出来添加到set中
            String sessionId = tv.getId();
            long serverTime = tv.getTime();
            List<Long> list = new ArrayList<>();
            // 存储时间
            if (map.containsKey(sessionId)) {
                list = map.get(sessionId);
                list.add(serverTime);
                map.put(sessionId, list);
            } else {
                list.add(serverTime);
                map.put(sessionId, list);
            }
        }

        // 计算session的时长
        int sessionLength = 0;
        for (Map.Entry<String, List<Long>> en : map.entrySet()) {
            List<Long> list = en.getValue();
            if (list.size() >= 2) {
                Collections.sort(list);
                sessionLength += (list.get(list.size() - 1) - list.get(0));
            }
        }
        if (sessionLength > 0 && sessionLength <= GlobalConstants.DAY_OF_MILISECONDS) {
            // 不足一秒算一秒
            if (sessionLength % 1000 == 0) {
                sessionLength = sessionLength / 1000;
            } else {
                sessionLength = sessionLength / 1000 + 1;
            }
        }
        // 构建输出的value
        MapWritable mapWritable = new MapWritable();
        mapWritable.put(new IntWritable(-1), new IntWritable(this.map.size()));
        mapWritable.put(new IntWritable(-2), new IntWritable(sessionLength));
        // 设置kpi
        this.v.setValue(mapWritable);
        this.v.setKpi(KpiType.valueOfType(key.getStatsCommonDimension().getKpiDimension().getKpiName()));
        // 输出
        context.write(key, this.v);
    }
}
