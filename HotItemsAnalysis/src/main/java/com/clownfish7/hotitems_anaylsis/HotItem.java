package com.clownfish7.hotitems_anaylsis;

import com.clownfish7.hotitems_anaylsis.beans.ItemViewCount;
import com.clownfish7.hotitems_anaylsis.beans.UserBehavior;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.calcite.shaded.com.google.common.collect.Lists;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.ArrayList;

/**
 * @author You
 * @create 2022-01-08 8:07 PM
 */
public class HotItem {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
//        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
//        env.getConfig().setAutoWatermarkInterval();


        env
                // 1. 读取数据
                .readTextFile("HotItemsAnalysis/src/main/resources/UserBehavior.csv")
                // 2. 转换为 pojo
                .map((MapFunction<String, UserBehavior>) line -> {
                    String[] split = line.split(",");
                    UserBehavior userBehavior = new UserBehavior();
                    userBehavior.setUserId(new Long(split[0]));
                    userBehavior.setItemId(new Long(split[1]));
                    userBehavior.setCategoryId(new Integer(split[2]));
                    userBehavior.setBehavior(split[3]);
                    userBehavior.setTimestamp(new Long(split[4]));
                    return userBehavior;
                })
                // 3. 分配时间戳和 watermark
                .assignTimestampsAndWatermarks(WatermarkStrategy
                                // 有序
                                .<UserBehavior>forMonotonousTimestamps()
                                // 无序
//                                .<UserBehavior>forBoundedOutOfOrderness(Duration.ofHours(1))
                                .withTimestampAssigner((SerializableTimestampAssigner<UserBehavior>) (element, recordTimestamp) -> {
//                                    System.out.println(recordTimestamp);
                                    return element.getTimestamp() * 1000L;
                                })
                )
                // 分组开创聚合得到每个窗口个各个商品的 count 值
                .filter(data -> "pv".equals(data.getBehavior()))
                .keyBy(UserBehavior::getItemId)
                .window(SlidingEventTimeWindows.of(Time.hours(1), Time.minutes(5)))
                .aggregate(new ItemCountAgg(), new WindowItemCountRestlt())

                //
                .keyBy(ItemViewCount::getWindowEnd)
                .process(new TopNHotItems(5))
                .print()
        ;

        env.execute("hot items anaylsis");

    }

    public static class ItemCountAgg implements AggregateFunction<UserBehavior, Long, Long> {

        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(UserBehavior value, Long accumulator) {
            return accumulator + 1;
        }

        @Override
        public Long getResult(Long accumulator) {
            return accumulator;
        }

        @Override
        public Long merge(Long a, Long b) {
            return a + b;
        }
    }

    // 定义全窗口函数
    public static class WindowItemCountRestlt implements WindowFunction<Long, ItemViewCount, Long, TimeWindow> {

        @Override
        public void apply(Long key, TimeWindow window, Iterable<Long> input, Collector<ItemViewCount> out) throws Exception {
            long count = 0L;
            for (Long v : input) {
                count += v;
            }
            out.collect(new ItemViewCount(key, window.getEnd(), count));
        }
    }

    private static class TopNHotItems extends KeyedProcessFunction<Long, ItemViewCount, String> {

        private Integer topSize;

        public TopNHotItems(Integer topSize) {
            this.topSize = topSize;
        }

        ListState<Tuple2<Long, Long>> itemViewCountListState;

        @Override
        public void open(Configuration parameters) throws Exception {
            itemViewCountListState = getRuntimeContext().getListState(
                    new ListStateDescriptor<>("itemViewCountListState", TypeInformation.of(new TypeHint<Tuple2<Long, Long>>() {
                    }))
            );
        }

        @Override
        public void processElement(ItemViewCount value, KeyedProcessFunction<Long, ItemViewCount, String>.Context ctx, Collector<String> out) throws Exception {
            itemViewCountListState.add(Tuple2.of(ctx.getCurrentKey(), value.getCount()));
            ctx.timerService().registerEventTimeTimer(value.getWindowEnd() + 1);
        }

        @Override
        public void onTimer(long timestamp, KeyedProcessFunction<Long, ItemViewCount, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
            StringBuilder builder = new StringBuilder();
            Long currentKey = ctx.getCurrentKey();
            ArrayList<Tuple2<Long, Long>> list = Lists.newArrayList(itemViewCountListState.get().iterator());
            list.sort((o1, o2) -> Long.compare(o2.f1, o1.f1));
            builder.append("====================================\n");

            for (int i = 0; i < Math.min(topSize, list.size()); i++) {
                builder.append("windowEnd=" + new Timestamp(timestamp - 1) + ", itemId=" + list.get(i).f0 + ", count=" + list.get(i).f1)
                        .append("\n");
            }

            out.collect(builder.toString());

        }

        @Override
        public void close() throws Exception {
            itemViewCountListState.clear();
        }
    }
}
