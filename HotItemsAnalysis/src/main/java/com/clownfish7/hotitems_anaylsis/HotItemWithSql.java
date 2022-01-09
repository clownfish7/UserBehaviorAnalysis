package com.clownfish7.hotitems_anaylsis;

import com.clownfish7.hotitems_anaylsis.beans.UserBehavior;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.*;

/**
 * @author You
 * @create 2022-01-08 8:07 PM
 */
public class HotItemWithSql {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
//        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
//        env.getConfig().setAutoWatermarkInterval();

        StreamTableEnvironment tabEnv = StreamTableEnvironment.create(env, EnvironmentSettings.newInstance().inStreamingMode().build());

        SingleOutputStreamOperator<UserBehavior> dataStream = env
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
                                    return element.getTimestamp() * 1000L;
                                })
                );


        Table dataTable = tabEnv.fromDataStream(dataStream, Schema.newBuilder()
                .column("itemId", DataTypes.BIGINT())
                .column("categoryId", DataTypes.INT())
                .column("behavior", DataTypes.STRING())
                .column("timestamp", DataTypes.BIGINT())
                .columnByMetadata("rowtime", DataTypes.TIMESTAMP_LTZ(3))
                .watermark("rowtime", "SOURCE_WATERMARK()")
                .build());


//        dataTable.executeInsert(TableDescriptor.forConnector("print").build());


        Table windowEndTable = dataTable
                .filter($("behavior").isEqual("pv"))
                .window(Slide.over(lit(1).hours()).every(lit(5).minutes()).on($("rowtime")).as("w"))
                .groupBy($("w"), $("itemId"))
                .select($("itemId"), $("w").end().as("windowEnd"), $("itemId").count().as("cnt"));

        // 经过查阅官网文档发现 rownumber 函数在tableAPi中不适用
        //.window(Over.partitionBy($("windowEnd")).orderBy($("cnt").desc()).as("ww"));

        // 这里踩坑了，如不使用来自 StreamTableEnvironment 中的方法去创建表后面会类型不一致不能执行，在坑里呆了好久 ~ 555
        DataStream<Row> windowEndStream = tabEnv.toDataStream(windowEndTable);
        tabEnv.createTemporaryView("windowEndTable", windowEndStream);

//        tabEnv.from("windowEndTable").printSchema();
//        tabEnv.from("windowEndTable").executeInsert(TableDescriptor.forConnector("print").build());

        /*tabEnv
                .executeSql(
                        "select * from (" +
                                "   select *, ROW_NUMBER() over(partition by windowEnd order by cnt desc) as row_num from windowEndTable" +
                                ") where row_num <= 5")

                .print();*/


        // ;------------------------------------------------------------------------------------------------------------

        // 纯 sql 实现
        tabEnv.createTemporaryView("dataTable", dataStream, Schema.newBuilder()
                .column("itemId", DataTypes.BIGINT())
                .column("categoryId", DataTypes.INT())
                .column("behavior", DataTypes.STRING())
                .column("timestamp", DataTypes.BIGINT())
                .columnByMetadata("rowtime", DataTypes.TIMESTAMP_LTZ(3))
                .watermark("rowtime", "SOURCE_WATERMARK()")
//                .watermark("timestamp", $("timestamp").minus(lit(5).seconds()))
                .build());


        tabEnv.executeSql(
                        "select * from (" +
                                "   select *, ROW_NUMBER() over(partition by windowEnd order by cnt desc) as row_num from ( " +
                                "        select itemId, count(itemId) as cnt, HOP_END(rowtime, interval '5' minute, interval '1' hour) as windowEnd" +
                                "        from dataTable" +
                                "        where behavior = 'pv' " +
                                "        group by itemId, HOP(rowtime, interval '5' minute, interval '1' hour)" +
                                "   )" +
                                ") where row_num <= 5")
                .print();


    }


}
