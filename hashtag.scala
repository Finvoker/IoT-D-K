// Databricks notebook source
import sys.process._
"wget -P /tmp https://www.datacrucis.com/media/datasets/stratahadoop-BCN-2014.json" !!

// COMMAND ----------

val localpath="file:/tmp/stratahadoop-BCN-2014.json"
dbutils.fs.mkdirs("dbfs:/datasets/")
dbutils.fs.cp(localpath, "dbfs:/datasets/")
display(dbutils.fs.ls("dbfs:/datasets/stratahadoop-BCN-2014.json"))

// COMMAND ----------

val df = sqlContext.read.json("dbfs:/datasets/stratahadoop-BCN-2014.json")

// COMMAND ----------

df.printSchema()

// COMMAND ----------

import scala.util.Sorting
import scala.collection.mutable.WrappedArray
//Question 1-3 hashtag count
val rddhashtag = df.select($"entities.hashtags.text").rdd.flatMap(i=>i.getAs[WrappedArray[String]](0))

// COMMAND ----------

val hashCounts = rddhashtag.map(word => (word,1)).reduceByKey(_+_)

// COMMAND ----------

//result for Q1-3
val topHashtags = hashCounts.top(hashCounts.count.toInt)(Ordering.by(_._2)).take(10).foreach(println)

// COMMAND ----------

//Question 2 most tweeter
val rddusr = df.select($"user".getField("id_str")).rdd.map(i=>i.getString(0))

// COMMAND ----------

val tweetCounts = rddusr.map(word => (word,1)).reduceByKey(_+_)

// COMMAND ----------

//Result for Q2
val mosttweetusr = tweetCounts.top(tweetCounts.count.toInt)(Ordering.by(x => x._2)).take(10).foreach(println)

// COMMAND ----------

//Question for hashtag trend
val rddtimehash = df.select($"entities.hashtags.text",$"created_at").rdd.map(i=>(i.getAs[WrappedArray[String]](0),i.getString(1)))

// COMMAND ----------

val hashtime = rddtimehash.map(i => i._1(0).toString+" "+i._2.toString)

// COMMAND ----------

hashtime.take(2).foreach(println)

// COMMAND ----------

val hashperday = hashtime.map(_.split(" ")).map(_.take(4)).map(_.mkString(" "))

// COMMAND ----------

val trendCounts = hashperday.map(word => (word,1)).reduceByKey(_+_)

// COMMAND ----------

trendCounts.count()

// COMMAND ----------

//result for final question,ranked in the form of (hashtag, date)
val toptrend = trendCounts.top(tweetCounts.count.toInt)(Ordering.by(x => x._2)).take(10).foreach(println)

// COMMAND ----------


