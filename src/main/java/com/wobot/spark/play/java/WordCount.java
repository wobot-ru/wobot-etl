package com.wobot.spark.play.java;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

public class WordCount {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("app");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> lines = sc.textFile("./data/negatives.txt");
        JavaRDD<String> words = lines.flatMap(line -> Arrays.asList(line.split(" ")));
        JavaPairRDD<String, Integer> counts = words
                .mapToPair(w -> new Tuple2<>(w, 1))
                .reduceByKey((x, y) -> x + y);

        for (Tuple2<String, Integer> tuple2: counts.collect()){
            System.out.println(tuple2._1 + ": " + tuple2._2);
        }

    }
}
