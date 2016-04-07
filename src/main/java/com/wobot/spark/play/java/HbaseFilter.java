package com.wobot.spark.play.java;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.time.LocalDate;
import java.time.Month;
import java.time.ZoneId;
import java.util.Date;
import java.util.List;


public class HbaseFilter {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("app");
        JavaSparkContext sc = new JavaSparkContext(conf);

        Configuration postConf = HBaseConfiguration.create();
        postConf.set("hbase.zookeeper.quorum", "127.0.0.1");
        postConf.set(TableInputFormat.INPUT_TABLE, "post");

        JavaPairRDD<ImmutableBytesWritable, Result> postRDD = sc.newAPIHadoopRDD(postConf,
                TableInputFormat.class,
                ImmutableBytesWritable.class,
                Result.class);

        long start = LocalDate.of(2016, Month.JANUARY, 1).atStartOfDay(ZoneId.systemDefault()).toEpochSecond();
        long end = LocalDate.of(2016, Month.MARCH, 31).atStartOfDay(ZoneId.systemDefault()).toEpochSecond();

        JavaPairRDD<ImmutableBytesWritable, Result> filteredPostRDD = postRDD
                .filter(x -> InDateRange(x._2.getValue("p".getBytes(), "post_date".getBytes()), start, end));

        JavaRDD<Tuple2<String, Date>> keyDatePostRDD = filteredPostRDD.map(x -> new Tuple2<>(
                        Bytes.toString(x._1.get()),
                        ToDate(x._2.getValue("p".getBytes(), "post_date".getBytes()))
                )
        );

        JavaRDD<Tuple2<String, Date>> sortedPostRDD = keyDatePostRDD.sortBy(x -> x._2, false, 1);

        List<Tuple2<String, Date>> list = sortedPostRDD.collect();

        System.out.println(list);



    }

    private static boolean InDateRange(byte[] value, long start, long end) {
        long val = Long.parseLong(Bytes.toString(value));
        return val >= start && val <= end;
    }

    private static Date ToDate(byte[] value){
        long val = Long.parseLong(Bytes.toString(value));
        return new Date(val * 1000);
    }
}
