package com.wobot.spark.play.java;


import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import scala.Tuple2;

import java.util.List;
import java.util.Map;


public class Hbase2Es {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("app");
        conf.set("es.nodes", "localhost:9200");

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


        JavaPairRDD<String, Map<String, String>> transformedProfileRDD = profileRDD.mapToPair(x -> new Tuple2<>(
                toString(x._1.get()),
                getProfileMap(x)
        ));

        JavaPairRDD<String, Map<String, String>> transformedPostRDD = postRDD.mapToPair(x -> new Tuple2<>(
                getString(x._2, "p", "profile_id"),
                getPostMap(x)
        ));


        JavaPairRDD<String, Map<String, ?>> mergedRDD = transformedProfileRDD
                .join(transformedPostRDD)
                .mapToPair(x -> mergeProfileWithPost(x._2._1, x._2._2))
                ;


        JavaEsSpark.saveToEsWithMeta(mergedRDD, "wobot2/post");

        List<Tuple2<String, Map<String, ?>>> output = mergedRDD.take(2);
        System.out.println(output);

    }

    private static Integer toInt(String str){
        try {
            return Integer.valueOf(str);
        }
        catch (NumberFormatException e){
            return 0;
        }
    }

    private static Boolean toBool(String str){
        try {
            return Boolean.valueOf(str);
        }
        catch (NumberFormatException e){
            return Boolean.FALSE;
        }
    }

    private static String toISO8601Date(int unixTime){
        return new DateTime(unixTime*1000L, DateTimeZone.UTC).toString();
    }

    private static String toString(byte[] val) {
        return Bytes.toString(val);
    }

    private static String getString(Result result, String columnFamily, String column) {
        return toString(result.getValue(columnFamily.getBytes(), column.getBytes()));
    }

    private static Map<String, String> getProfileMap(Tuple2<ImmutableBytesWritable, Result> x) {
        return ImmutableMap.<String, String>builder()
                .put("id", toString(x._1.get()))
                .put("source", getString(x._2, "p", "source"))
                .put("href", getString(x._2, "p", "href"))
                .put("sm_profile_id", getString(x._2, "p", "sm_profile_id"))
                .put("name", getString(x._2, "p", "name"))
                .put("city", getString(x._2, "p", "city"))
                .put("reach", getString(x._2, "p", "reach"))
                .put("gender", getString(x._2, "p", "gender"))
                .build();
    }

    private static Map<String, String> getPostMap(Tuple2<ImmutableBytesWritable, Result> x) {
        return ImmutableMap.<String, String>builder()
                .put("id", toString(x._1.get()))
                .put("source", getString(x._2, "p", "source"))
                .put("profile_id", getString(x._2, "p", "profile_id"))
                .put("href", getString(x._2, "p", "href"))
                .put("sm_post_id", getString(x._2, "p", "sm_post_id"))
                .put("body", getString(x._2, "p", "body"))
                .put("post_date", getString(x._2, "p", "post_date"))
                .put("engagement", getString(x._2, "p", "engagement"))
                .put("is_comment", getString(x._2, "p", "is_comment"))
                .put("parent_post_id", getString(x._2, "p", "parent_post_id"))
                .build();
    }

    private static Tuple2<String, Map<String, ?>> mergeProfileWithPost(Map<String, String> profile, Map<String, String> post) {
        Map<String, ?> body = ImmutableMap.<String, Object>builder()
                .put("id", post.get("id"))
                .put("source", post.get("source"))
                .put("profile_id", profile.get("id"))
                .put("sm_profile_id", profile.get("sm_profile_id"))
                .put("profile_name", profile.get("name"))
                .put("profile_href", profile.get("href"))
                .put("profile_city", profile.get("city"))
                .put("profile_gender", profile.get("gender"))
                .put("post_href", post.get("href"))
                .put("sm_post_id", post.get("sm_post_id"))
                .put("post_body", post.get("body"))
                .put("post_date", toISO8601Date( toInt(post.get("post_date")) ) )
                .put("engagement", toInt(post.get("engagement")))
                .put("reach", toInt(profile.get("reach")))
                .put("is_comment", toBool(post.get("is_comment")))
                .put("parent_post_id", post.get("parent_post_id"))
                .build();

        String key = post.get("id");

        return new Tuple2<>(key, body);
    }
}