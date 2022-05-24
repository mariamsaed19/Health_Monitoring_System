package org.example;

import org.apache.commons.io.FileUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.internal.config.R;
import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;


import javax.xml.crypto.Data;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeoutException;

import static org.apache.spark.sql.functions.*;

/*
run maven package then
FROM CMD NOT INTELLIJ
YOUR_SPARK_HOME/bin/spark-submit \
  --class "SimpleApp" \
  --master local[4] \ hadoop-namenode:9820
  target/simple-project-1.0.jar


System.getenv()
  spark-submit --class "org.example.RealTime" --master local[4] RealTime-1.0.jar localhost 10101


  Command to run:  D:\spark\bin\spark-submit --class "org.example.RealTime" --master local[4] RealTime-1.0.jar localhost 10101 /path/to/parquet /path/to/checkpoint
*/
public class RealTime {

    public static void main(String[] args) throws StreamingQueryException, TimeoutException {

        if (args.length < 4){
            throw new RuntimeException("Not enough arguments");
        }

        System.out.println(args[0]);
        System.out.println(args[1]);
        Runtime.getRuntime().addShutdownHook(
                new Thread(() -> {
                    try {
                        FileUtils.deleteDirectory(new File(args[2]));
                        FileUtils.deleteDirectory(new File(args[3]));
                    } catch (IOException e) {
                        e.printStackTrace();
                    }

                })
        );

        SparkSession spark = SparkSession
                .builder()
                .config("spark.sql.streaming.statefulOperator.checkCorrectness.enabled", "false")
                .getOrCreate();
        Dataset<Row> socketDF = spark.readStream()
                .format("socket")
                .option("host", args[0])
                .option("port", Integer.parseInt(args[1])) // Either we make the monitor send to two ports (one per hour, then switch to another once the spark job finishes
                .load(); // Or we write to a file ... hmm?

        /* create proper table*/
        socketDF = socketDF.selectExpr("split(value,',')[0] as serviceName",
                "timestamp(int(split(value,',')[1])) as time_stamp",
                "double(split(value,',')[2]) as cpu",
                "double(split(value,',')[3]) as ram_tot",
                "double(split(value,',')[4]) as ram_free",
                "double(split(value,',')[5]) as disk_tot",
                "double(split(value,',')[6]) as disk_free");
        socketDF = socketDF.selectExpr("serviceName","time_stamp", "cpu",
                "(ram_tot-ram_free)/ram_tot as ram_ut", "(disk_tot-disk_free)/disk_tot as disk_ut");

        socketDF.printSchema();

        /* watermarked */
        Dataset<Row> watermarked = socketDF.withWatermark("time_stamp","1 minute");

        /* aggregate */
        Dataset<Row> aggDF= watermarked
                .groupBy(
                    window(col("time_stamp"),"1 minute"),
                    col("serviceName").as("service_name")
                ).agg(
                        avg("cpu").as("avg_cpu")
                        ,avg("ram_ut").as("avg_ram")
                        ,avg("disk_ut").as("avg_disk")
                        ,max("cpu").as("cpu_peak_util")
                        ,max("disk_ut").as("disk_peak_util")
                        ,max("ram_ut").as("ram_peak_util")
                        ,count("time_stamp").as("count")
                );
        //aggDF.createOrReplaceTempView("aggDF");
        aggDF.printSchema();
        aggDF = aggDF.select(
                from_unixtime(col("window.start").cast("bigint"), "yyyy_MM_dd_HH_mm").as("Timestamp"),
                col("service_name"),
                col("avg_cpu"),
                col("avg_ram"),
                col("avg_disk"),
                col("cpu_peak_util"),
                col("disk_peak_util"),
                col("ram_peak_util"),
                col("count")
        ).join(
                watermarked,
                expr("service_name = serviceName")
        );

        StreamingQuery query = aggDF
                .writeStream()
                .format("parquet")
                .option("path", args[2]) //TODO args[2] hdfs://
                .option("checkpointLocation", args[3]) //TODO args[3]
                .start();

        query.awaitTermination();

    }
}
