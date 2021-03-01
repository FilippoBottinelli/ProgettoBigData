package bigData

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.DateTime
import org.apache.spark.sql.functions.{col, count, explode}



object JsonParser {

  val conf = new SparkConf().setMaster("local[2]").setAppName("TabellaQuery")
  val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)
  val hiveContext = new HiveContext(sc)
  val input = "E:\\Roba\\fileBigData\\Firs500Rows.json"

  import hiveContext.implicits._
  def main(args: Array[String]) {

    //parsing di Firs500Rows
    val myDataFrameEvent = sqlContext.read.json(input)
    val myDataFrameEventParsed = myDataFrameEvent.withColumnRenamed("public", "publicField")

    //dichiarazione dataset di tipo event
    val myDataSetEvent = myDataFrameEventParsed.as[Event]
    //dichiarazione rdd di tipo event
    val myRddEvent = myDataSetEvent.rdd

    //slide 1 ex 1
    /////////////////////////// trovo actor (senza ripetizioni) ////////////////////////////////////////////////////////

    //dataframe
    val myDataFrameActor = myDataFrameEventParsed.select("actor").distinct() //distinct rimuove le ripetizioni
    myDataFrameActor.show() //stampo dataframe di actor

    //rdd
    val myRddActor = myRddEvent.map(x => x.actor).distinct()
    myRddActor.take(10).foreach(println)

    //slide 1 ex 2
    /////////////////////////// trovo author contenuti in commit (senza ripetizioni) ///////////////////////////////////

    //dataframe
    val myDataFramePayload = myDataFrameEvent.select("payload.*") //l'asterisco significa "prendi tutto cio che ce in payload"
    val myDataFrameCommit = myDataFramePayload.select(explode(col("commits"))).select("col.*")
    myDataFrameCommit.select("author").show()

    //rdd
    val myRddCommit = myDataFrameCommit.as[Commit].rdd
    val myRddAuthor = myRddCommit.map(x => x.author).distinct()
    myRddAuthor.take(10).foreach(println)

    //slide 1 ex 3
    /////////////////////////// trovo i repo (senza ripetizioni) ///////////////////////////////////////////////////////

    //dataframe
    val myDataFrameRepo = myDataFrameEventParsed.select("repo").distinct()
    myDataFrameRepo.show()

    //rdd
    val myRddRepo = myRddEvent.map(x => x.repo).distinct()
    myRddRepo.take(10).foreach(println)

    //slide 1 ex 4
    /////////////////////////// trovo tipologie di evento type /////////////////////////////////////////////////////////

    //dataframe
    val myDataFrameType = myDataFrameEventParsed.select("`type`").distinct()
    myDataFrameType.show()

    //rdd
    val myRddType = myRddEvent.map(x => x.`type`).distinct()
    myRddType.take(10).foreach(println)

    //slide 1 ex 5
    /////////////////////////// conto gli actor ////////////////////////////////////////////////////////////////////////

    //dataframe
    println( myDataFrameEvent.select("actor").distinct().count()) //count restituisce un conteggio


    //rdd
    println(myRddEvent.map(x => x.actor).distinct().count())

    //slide 1 x 6
    /////////////////////////// conto i repo ///////////////////////////////////////////////////////////////////////////

    //dataframe
    println(myDataFrameEvent.select("repo").distinct().count())

    //rdd
    println(myRddEvent.map(x => x.repo).distinct().count())

    //slide 2 ex 1
    /////////////////////////// conto gli event per ogni actor /////////////////////////////////////////////////////////

    //dataframe
    println(myDataFrameEvent.select("actor").count())

    //rdd
    println(myRddEvent.map(x => x.actor).count())

    //slide 2 ex 2
    /////////////////////////// conto event divisi per type e actor ////////////////////////////////////////////////////

    //dataframe
    myDataFrameEvent.select(col("type"), col("actor"), count($"").over(Window.partitionBy("type", "actor")) as "nEvent").show()

    //rdd
    val rddEvent = myRddEvent.map(x => ((x.`type`, x.actor), 1L)).reduceByKey((e1,e2) => e1+e2)
    rddEvent.take(10).foreach(println)

    //slide 2 ex 3
    /////////////////////////// conto gli event divisi per type actor e repo ///////////////////////////////////////////

    //dataframe
    myDataFrameEvent.select(col("type"), col("actor"), col("repo"), count($"").over(Window.partitionBy(col("type"), col("actor"), col("repo"))) as "nEvent").show()

    //rdd
    myRddEvent.map(x => ((x.`type`, x.actor, x.repo), 1L)).reduceByKey((e1,e2) => e1+e2).take(10).foreach(println)

  }
}