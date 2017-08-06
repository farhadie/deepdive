package diuf.exascale.deepdive.SparkConnector

/**
  * Spark connector for make SQL commands compatible for spark
  * for DeepDive-spark integration
  * by: Ehsan
  * last update: 28/7/2017
*/

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql._

object SparkConnector {
  def main(args: Array[String]): Unit = {
    val usage = """
    Usage: SparConnector "SQL-Query"
    """
    if (args.length != 1) {println(usage); sys.exit(1)}

    val spark = SparkSession
      .builder()
      .appName("DeepDive_wrapper")
      // compress parquet datasets with snappy
      .config("spark.sql.parquet.compression.codec", "snappy")
      .getOrCreate()
    //parse args
    val t0 = System.nanoTime()//time measurement
    println(args.mkString(" -- "))
    val parsed = util.parse_args(args)
    val hadoop_dir:String = parsed._1
    val sql_command:String = parsed._2
    val sql_args:Array[String] = parsed._3
    // sql command match case :
    sql_command match {
      case "CREATE" =>
        val create_type = sql_args(1)
        val name = sql_args(2)
        //println(sql_args.mkString(" "))
        println(create_type)
        // "create table like":
        create_type match {
          case "TABLE" =>
            println("Creating table...")
            if (sql_args(4) == "AS"){
              val query = util.parse_query(sql_args.drop(5).mkString(" "))
              println(query)
              util.load_tables(query, spark, hadoop_dir)
              val table = spark.sql(query)
              table.write.format("parquet").save("/"+hadoop_dir+"/"+name+"_tmp")
              println("creating complete.")
            }
            else if (sql_args(4).replace("(", "")=="LIKE"){
              val like_table = spark.read.parquet("/"+hadoop_dir+"/"+sql_args(5))
              val df = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], like_table.schema)
              // create a table with 1 emtpy row
              val rdd = spark.sparkContext.parallelize(Seq(List.fill(df.schema.fieldNames.length)("")))
              val rowRdd = rdd.map(v => Row(v: _*))
              val newdf  = spark.createDataFrame(rowRdd, df.schema)
              newdf.write.format("parquet").save("/"+hadoop_dir+"/"+name+"_tmp")
              newdf.show()
              println("creating complete.")
            }
            // "create table"
            else{
              val columns = util.makecolumns(sql_args.drop(3).mkString(" "))
              // create a table with 1 emtpy row
              val tableDD = spark.sparkContext.makeRDD(s"""{$columns}""" :: Nil)
              val table = spark.read.json(tableDD)
              val order = util.reorder(sql_args.drop(3).mkString(" "))
              val table_reordered = table.select(order.head, order.tail:_*)
              table_reordered.show()
              table_reordered.printSchema()
              table_reordered.write.format("parquet").save("/"+hadoop_dir+"/"+name+"_tmp")
              println("creating complete.")
            }
          case "VIEW" =>
            println("Creating view...")
            if (sql_args(4) == "AS"){
              val query = util.parse_query(sql_args.drop(5).mkString(" "))
              println(query)
              util.load_tables(query, spark, hadoop_dir)
              val table = spark.sql(query)
              table.write.format("parquet").save("/"+hadoop_dir+"/"+name+"_tmp")
              println("creating complete.")
            }
          case _ =>
            println("Not defined yet")
        }
      // -- CREATE
      case "LOAD" | "\\COPY" =>
        println("loading data...")
        println("finding table: "+"/"+hadoop_dir+"/"+sql_args(0))
        var table:DataFrame=null
        try {
          table = spark.read.parquet ("/" + hadoop_dir + "/" + sql_args (0) )
        } catch {case e: Exception => println(e);sys.exit(1);}
        // read data from input tsv format
        println("tables found.")
        var data:DataFrame=null
        println("loading data...")
        try {
          data = spark.sqlContext.read.format("csv")
            .option("delimiter", "\t")
            .load(sql_args(3))
        }catch {case e:Exception => println(e);sys.exit(1);}
        println("loading done.")
        //add table column names and save
        val fields = table.schema.fieldNames
        val last = data.toDF(fields:_*)
        last.write.mode("append").format("parquet").save("/"+hadoop_dir+"/"+sql_args(0)+"_tmp")
        println("loading complete.")
        // -- LOAD
      case "ASSIGNID" =>
        println(sql_args.mkString(" "))
        var last_id:Long = 0
        sql_args(0).split(",").foreach{
          r => {
            val df = spark.read.load("/"+hadoop_dir+"/"+r)
            val dropped = df.drop(df.col(sql_args(1)))
            val output = dropped.withColumn(sql_args(1), monotonicallyIncreasingId+last_id)
            last_id = last_id + output.count
            output.write.format("parquet").save("/"+hadoop_dir+"/"+r+"_tmp")
            println("update IDs of:"+r)
          }
        }
      case _ =>
        println("Not defined yet")
        println(sql_command, sql_args.mkString(" "))
    }
    val t1 = System.nanoTime() //time measurement
    println("Elapsed time: " + (t1 - t0) + "ns")
  }
}
