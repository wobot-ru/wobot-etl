package com.wobot.spark.play.java;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;
import scala.Tuple2;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;


public class Es2Es {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("app");
        conf.set("es.nodes", "localhost:9200");

        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaPairRDD<String, Map<String, Object>> profileRDD = JavaEsSpark.esRDD(sc, "wobot1/profile");
        JavaPairRDD<String, Map<String, Object>> postRDD = JavaEsSpark.esRDD(sc, "wobot1/post");

        JavaPairRDD<String, Map<String, Object>> transformedPostRDD = postRDD.mapToPair(w -> new Tuple2<>(
                w._2.get("profile_id").toString(),
                w._2
        ));

        JavaPairRDD<String, Map<String, Object>> joinedRDD = profileRDD
                .join(transformedPostRDD)
                .mapToPair(x -> mergeProfileWithPost(x._2._1, x._2._2));

        JavaEsSpark.saveToEsWithMeta(joinedRDD, "wobot3/post");

        List<Tuple2<String, Map<String, Object>>> joined = joinedRDD.take(1);
        System.out.println(joined);

    }

    private static Tuple2<String, Map<String, Object>> mergeProfileWithPost(Map<String, Object> profile, Map<String, Object> post) {

        Map<String, Object> body = new LinkedHashMap<>();

        if (post.containsKey("id"))
            body.put("id", post.get("id"));

        if (post.containsKey("source"))
            body.put("source", post.get("source"));

        if (profile.containsKey("id"))
            body.put("profile_id", profile.get("id"));

        if (profile.containsKey("sm_profile_id"))
            body.put("sm_profile_id", profile.get("sm_profile_id"));

        if (profile.containsKey("name"))
            body.put("profile_name", profile.get("name"));

        if (profile.containsKey("href"))
            body.put("profile_href", profile.get("href"));

        if (profile.containsKey("city"))
            body.put("profile_city", profile.get("city"));

        if (profile.containsKey("gender"))
            body.put("profile_gender", profile.get("gender"));

        if (post.containsKey("href"))
            body.put("post_href", post.get("href"));

        if (post.containsKey("sm_post_id"))
            body.put("sm_post_id", post.get("sm_post_id"));

        if (post.containsKey("body"))
            body.put("post_body", post.get("body"));

        if (post.containsKey("post_date"))
            body.put("post_date", post.get("post_date"));

        if (post.containsKey("engagement"))
            body.put("engagement", post.get("engagement"));

        if (profile.containsKey("reach"))
            body.put("reach", profile.get("reach"));

        if (post.containsKey("is_comment"))
            body.put("is_comment", post.get("is_comment"));

        if (post.containsKey("parent_post_id"))
            body.put("parent_post_id", post.get("parent_post_id"));

        String key = post.get("id").toString();

        return new Tuple2<>(key, body);
    }
}
