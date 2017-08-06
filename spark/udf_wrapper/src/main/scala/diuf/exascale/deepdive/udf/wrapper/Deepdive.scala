package diuf.exascale.deepdive.udf.wrapper

/**
  * Basic class for UDFs.
  * Created by Ehsan on 3/3/17.
  */

import org.apache.spark.sql.{DataFrame, SparkSession}

class Deepdive {
  var saved_args:Array[String]=_
  // create views ready for sql queries in spark session
  def load_tables(args: Array[String], spark: SparkSession): Unit = {
    saved_args = args
    val hadoop_dir = args(0)
    val input_sql = args(1)
    val output_relation = args(2)

    println(input_sql)
    println(output_relation)

    val from = input_sql.split("FROM ")(1)
    val relations = from.split(", ")
    for (relation <- relations){
      val relation_name = relation.split(" ")(0)
      val df = spark.read.load("/"+hadoop_dir+"/"+relation_name)
      df.createOrReplaceTempView(relation_name)
      println("load: /"+hadoop_dir+"/"+relation_name)
    }
  }

  // save output to HDFS
  def save(result: DataFrame):Unit = {
    val output_directory = saved_args(0)
    val table_name = saved_args(2)
    println("saving output to: /"+output_directory+"/"+table_name+"_tmp")
    result.write.mode("append").format("parquet").save("/"+output_directory+"/"+table_name+"_tmp")
    println("saving done.")
  }
}
