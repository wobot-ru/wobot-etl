package com.wobot.spark.play.java;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.List;


public class Hbase {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("app");
        JavaSparkContext sc = new JavaSparkContext(conf);

        Configuration profileConf = HBaseConfiguration.create();
        profileConf.set("hbase.zookeeper.quorum", "127.0.0.1");
        profileConf.set(TableInputFormat.INPUT_TABLE, "profile");

        Configuration postConf = HBaseConfiguration.create();
        postConf.set("hbase.zookeeper.quorum", "127.0.0.1");
        postConf.set(TableInputFormat.INPUT_TABLE, "post");


        JavaPairRDD<ImmutableBytesWritable, Result> profileRDD = sc.newAPIHadoopRDD(profileConf,
                TableInputFormat.class,
                ImmutableBytesWritable.class,
                Result.class);

        JavaPairRDD<ImmutableBytesWritable, Result> postRDD = sc.newAPIHadoopRDD(postConf,
                TableInputFormat.class,
                ImmutableBytesWritable.class,
                Result.class);


        JavaPairRDD<String, String> transformedProfileRDD = profileRDD.mapToPair(x -> new Tuple2<>(
                Bytes.toString(x._1.get()),
                Bytes.toString(x._2.getValue("p".getBytes(), "name".getBytes()))
        ));

        JavaPairRDD<String, String> transformedPostRDD = postRDD.mapToPair(x -> new Tuple2<>(
                Bytes.toString(x._2.getValue("p".getBytes(), "profile_id".getBytes())),
                Bytes.toString(x._2.getValue("p".getBytes(), "body".getBytes()))
        ));

        //(profile_id, (profile, post))
        JavaPairRDD<String, Tuple2<String, String>> joined = transformedProfileRDD.join(transformedPostRDD);

        List<Tuple2<String, Tuple2<String, String>>> output = joined.take(10);
        System.out.println(output);


    }
}
