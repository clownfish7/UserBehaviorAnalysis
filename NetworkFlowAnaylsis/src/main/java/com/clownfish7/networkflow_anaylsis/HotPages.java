package com.clownfish7.networkflow_anaylsis;

import com.clownfish7.networkflow_anaylsis.beans.ApacheLogEvent;
import com.clownfish7.networkflow_anaylsis.beans.PageViewCount;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.net.URL;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

/**
 * @author You
 * @create 2022-01-09 11:31 PM
 */
public class HotPages {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        URL resource = HotPages.class.getResource("/apache.log");
        env
                .readTextFile(resource.getPath())
                .map(line -> {
                    // 93.114.45.13 - - 17/05/2015:10:05:14 +0000 GET /articles/dynamic-dns-with-dhcp/
                    String[] fields = line.split(" ");
                    LocalDateTime localDateTime = LocalDateTime.parse(fields[3], DateTimeFormatter.ofPattern("dd/MM/yyyy:HH:mm:ss"));
                    long timestamp = localDateTime.toInstant(ZoneOffset.of("+8")).toEpochMilli();
                    // String ip, String userId, Long timestamp, String method, String url
                    return new ApacheLogEvent(fields[0], fields[1], timestamp, fields[5], fields[6]);
                })
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<ApacheLogEvent>forBoundedOutOfOrderness(Duration.ofMinutes(1))
                                .withTimestampAssigner((element, timestamp) -> element.getTimestamp())
                )


                // 分组开创聚合
                .filter(log -> "GET".equals(log.getMethod()))
                .keyBy(ApacheLogEvent::getUrl)
                .window(SlidingEventTimeWindows.of(Time.minutes(10), Time.seconds(5)))
                .aggregate(new PageCountAgg(), new PageCountResult())


                // 收集同一窗口数据，排序输出
                .keyBy(PageViewCount::getWindowEnd)
                .process(new TopNHotPages(5))
                .print()
        ;

        env.execute("hot pages job");
    }

    public static class PageCountAgg implements AggregateFunction<ApacheLogEvent, Long, Long> {

        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(ApacheLogEvent value, Long accumulator) {
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

    public static class PageCountResult implements WindowFunction<Long, PageViewCount, String, TimeWindow> {

        @Override
        public void apply(String key, TimeWindow window, Iterable<Long> input, Collector<PageViewCount> out) throws Exception {
            long count = 0;
            for (Long c : input) {
                count += c;
            }
            PageViewCount record = new PageViewCount(key, window.getEnd(), count);
            out.collect(record);
        }
    }

    private static class TopNHotPages extends KeyedProcessFunction<Long, PageViewCount, String> {

        private int topSize;

        public TopNHotPages(int topSize) {
            this.topSize = topSize;
        }

        @Override
        public void processElement(PageViewCount value, KeyedProcessFunction<Long, PageViewCount, String>.Context ctx, Collector<String> out) throws Exception {

        }
    }
}
