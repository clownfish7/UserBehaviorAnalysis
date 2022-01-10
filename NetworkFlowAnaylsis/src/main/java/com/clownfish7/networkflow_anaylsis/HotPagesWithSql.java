package com.clownfish7.networkflow_anaylsis;

import com.clownfish7.networkflow_anaylsis.beans.ApacheLogEvent;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.net.URL;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

/**
 * classname HotPagesWithSql
 * description TODO
 * create 2022-01-10 16:34
 */
public class HotPagesWithSql {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
//        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
//        env.getConfig().setAutoWatermarkInterval();

        StreamTableEnvironment tabEnv = StreamTableEnvironment.create(env, EnvironmentSettings.newInstance().inStreamingMode().build());

        URL resource = HotPagesWithSql.class.getResource("/apache.log");
        SingleOutputStreamOperator<ApacheLogEvent> dataStream = env
                // 1. 读取数据
                .readTextFile(resource.getPath())
                // 2. 转换为 pojo
                .map(line -> {
                    // 93.114.45.13 - - 17/05/2015:10:05:14 +0000 GET /articles/dynamic-dns-with-dhcp/
                    String[] fields = line.split(" ");
                    LocalDateTime localDateTime = LocalDateTime.parse(fields[3], DateTimeFormatter.ofPattern("dd/MM/yyyy:HH:mm:ss"));
                    long timestamp = localDateTime.toInstant(ZoneOffset.of("+8")).toEpochMilli();
                    // String ip, String userId, Long timestamp, String method, String url
                    return new ApacheLogEvent(fields[0], fields[1], timestamp, fields[5], fields[6]);
                })
                // 3. 分配时间戳和 watermark
                .assignTimestampsAndWatermarks(WatermarkStrategy
                                // 有序
//                                .<ApacheLogEvent>forMonotonousTimestamps()
                                // 无序
                                .<ApacheLogEvent>forBoundedOutOfOrderness(Duration.ofMinutes(1))
                                .withTimestampAssigner((SerializableTimestampAssigner<ApacheLogEvent>)
                                        (element, recordTimestamp) -> element.getTimestamp())
                );

        tabEnv.createTemporaryView("dataTable", dataStream, Schema.newBuilder()
                .column("ip", DataTypes.STRING())
                .column("userId", DataTypes.STRING())
                .column("timestamp", DataTypes.BIGINT())
                .column("method", DataTypes.STRING())
                .column("url", DataTypes.STRING())
                .columnByMetadata("rowtime", DataTypes.TIMESTAMP_LTZ(3))
                .watermark("rowtime", "SOURCE_WATERMARK()")
                .build());


        tabEnv.executeSql("select * from (" +
                        "               select *, ROW_NUMBER() OVER (partition by windowEnd order by cnt desc) as row_num from (" +
                        "                   select url, count(url) as cnt, HOP_START(rowtime, interval '5' second, interval '10' minute) as windowStart, HOP_END(rowtime, interval '5' second, interval '10' minute) as windowEnd " +
                        "                   from dataTable " +
                        "                   group by HOP(rowtime, interval '5' second, interval '10' minute), url" +
                        "               )" +
                        "           ) where row_num <= 5")
                .print();

    }
}
