package com.wobot.spark.play.scala

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.{SparkConf, SparkContext}

object Hbase {
    def main(args: Array[String]) {

        val conf = new SparkConf().setMaster("local[4]").setAppName("app")
        val sc = new SparkContext(conf)

        val profileConf = HBaseConfiguration.create()
        profileConf.set("hbase.zookeeper.quorum", "127.0.0.1")
        profileConf.set(TableInputFormat.INPUT_TABLE, "profile")

        val postConf = HBaseConfiguration.create()
        postConf.set("hbase.zookeeper.quorum", "127.0.0.1")
        postConf.set(TableInputFormat.INPUT_TABLE, "post")

        val profileRDD = sc.newAPIHadoopRDD(profileConf, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])
        val postRDD = sc.newAPIHadoopRDD(postConf, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])

        val transformedProfileRDD = profileRDD.map(x => (
                Bytes.toString(x._1.get()),
                Bytes.toString(x._2.getValue("p".getBytes(), "name".getBytes()))
                )
        )

        val transformedPostRDD = postRDD.map(x => (
                Bytes.toString(x._2.getValue("p".getBytes(), "profile_id".getBytes())),
                Bytes.toString(x._2.getValue("p".getBytes(), "body".getBytes()))
                )
        )

        val joined = transformedProfileRDD.join(transformedPostRDD)
        val output = joined.take(10)

        println(output)

    }
}
