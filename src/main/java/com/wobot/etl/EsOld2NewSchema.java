package com.wobot.etl;

import org.apache.spark.HashPartitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;
import scala.Tuple2;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.Serializable;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;

import static java.util.Arrays.stream;

public class EsOld2NewSchema {

    public static void main(String[] args) {

        Properties properties = loadProperties();
        String[] nodes = properties.getProperty("es.nodes", "91.210.104.84").split(",");
        String cluster = properties.getProperty("es.cluster", "wobot-cluster");
        String sourceIndex = properties.getProperty("es.source", "wobot_fb");
        String destIndex = properties.getProperty("es.destination", "wobot33");
        Integer bulkSize = Integer.valueOf(properties.getProperty("es.bulkSize", "5000"));
        Integer numPartitions = Integer.valueOf(properties.getProperty("spark.partitions", "5"));

        SparkConf conf = new SparkConf();

        if (!conf.contains("spark.master")) {
            conf.set("spark.master", "local");
        }

        if (!conf.contains("spark.app.name")) {
            conf.set("spark.app.name", "es2es");
        }

        conf.set("spark.driver.userClassPathFirst", "true");
        conf.set("spark.executor.userClassPathFirst", "true");
        conf.set("es.nodes", stream(nodes).map(x -> x + ":9200").reduce((x, y) -> x + "," + y).get());


        HashPartitioner partitioner = new HashPartitioner(numPartitions);

        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaPairRDD<String, Map<String, Object>> postRDD = JavaEsSpark.esRDD(sc, sourceIndex + "/post", "?q=post_date:[now-12M/d TO now]");

        JavaPairRDD<String, Map<String, Object>> profileRDD = JavaEsSpark.esRDD(sc, sourceIndex + "/profile")
                .partitionBy(partitioner);

        //(profileId, (postId, post))
        JavaPairRDD<String, Tuple2<String, Map<String, Object>>> transformedPostRDD = postRDD
                .mapToPair(w -> new Tuple2<>(
                        w._2.get("profile_id").toString(),
                        new Tuple2<>(w._1, w._2)
                ))
                .partitionBy(partitioner);

        //(profileId, ((postId, post), profile))
        JavaPairRDD<String, Tuple2<Tuple2<String, Map<String, Object>>, Map<String, Object>>> joined = transformedPostRDD
                .join(profileRDD);

        //(postId, post)
        JavaPairRDD<String, Map<String, Object>> merged = joined.mapToPair(x -> new Tuple2<>(
                x._2._1._1,
                EsPostMerger.merge(x._2._2, x._2._1._2, x._2._1._1)
        ));


        EsWriter writer = new EsWriter(nodes, cluster, destIndex, bulkSize);
        merged.foreachPartition(documents -> writer.Write(documents));

        println("----------------partitions----------------------");
        println(merged.partitions().size());
        println("-----------------complete----------------------");

        sc.stop();

    }

    private static Properties loadProperties() {
        Properties props = new Properties();

        try {
            props.load(new FileInputStream("./conf.properties"));
        } catch (IOException e) {
            e.printStackTrace();
        }


        return props;
    }

    static <T> void println(T arg) {
        System.out.println(arg);
    }

}

class EsPostMerger{
    static Map<String, Object> merge(Map<String, Object> profile, Map<String, Object> post, String id) {

        Map<String, Object> result = new LinkedHashMap<>();

        result.put("id", id);

        if (post.containsKey("source"))
            result.put("source", post.get("source"));

        if (post.containsKey("profile_id"))
            result.put("profile_id", post.get("profile_id"));

        if (profile.containsKey("sm_profile_id"))
            result.put("sm_profile_id", profile.get("sm_profile_id"));

        if (profile.containsKey("name"))
            result.put("profile_name", profile.get("name"));

        if (profile.containsKey("href"))
            result.put("profile_href", profile.get("href"));

        if (profile.containsKey("city"))
            result.put("profile_city", profile.get("city"));

        if (profile.containsKey("gender"))
            result.put("profile_gender", profile.get("gender"));

        if (post.containsKey("href"))
            result.put("post_href", post.get("href"));

        if (post.containsKey("sm_post_id"))
            result.put("sm_post_id", post.get("sm_post_id"));

        if (post.containsKey("body"))
            result.put("post_body", post.get("body"));

        if (post.containsKey("post_date"))
            result.put("post_date", post.get("post_date"));

        if (post.containsKey("engagement"))
            result.put("engagement", post.get("engagement"));

        if (profile.containsKey("reach"))
            result.put("reach", profile.get("reach"));

        if (post.containsKey("is_comment"))
            result.put("is_comment", post.get("is_comment"));

        if (post.containsKey("parent_post_id"))
            result.put("parent_post_id", post.get("parent_post_id"));

        return result;
    }
}

class EsWriter implements Serializable {

    private final String[] nodes;
    private final String cluster;
    private final String destIndex;
    private final Integer bulkSize;

    EsWriter(String[] nodes, String cluster, String destIndex, Integer bulkSize) {
        this.nodes = nodes;
        this.cluster = cluster;
        this.destIndex = destIndex;
        this.bulkSize = bulkSize;
    }

    void Write(Iterator<Tuple2<String, Map<String, Object>>> documents) throws UnknownHostException {
        Settings settings = Settings.settingsBuilder().put("cluster.name", cluster).build();
        TransportClient client = TransportClient
                .builder()
                .settings(settings)
                .build();

        for (String node : nodes) {
            client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(node), 9300));
        }

        BulkRequestBuilder bulk = client.prepareBulk();

        int i = 0;
        while (documents.hasNext()) {

            Tuple2<String, Map<String, Object>> doc = documents.next();
            bulk.add(client.prepareIndex(destIndex, "post", doc._1).setSource(doc._2));
            i++;

            if (i >= bulkSize) {
                bulk.execute().actionGet();
                bulk = client.prepareBulk();
                i = 0;
            }
        }

        bulk.execute().actionGet();

        client.close();
    }
}
