package diuf.exascale.deepdive.SparkConnector

import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ListBuffer
import scala.util.matching.Regex

/**
  * args parser
  * by: Ehsan
  * last update: 28/7/2017
  */
object util {
  def parse_args(args: Array[String]):(String, String, Array[String])={
    val inputs = args.head.split(" ")
    (inputs(0), inputs(1), inputs.drop(2))
  }

  def parse_query(query: String):String = {
    var q = query
    q = q.replaceAll("(?<=\")([a-zA-Z0-9\\.\\_]+)(\\.)([a-zA-Z0-9\\.\\_]+)(?=\")", "$1_$3") //LMAO
    q = q.replaceAll("(?<=\")([a-zA-Z0-9\\.\\_]+)(\\.)([a-zA-Z0-9\\.\\_]+)(?=\")", "$1_$3")
    q = q.replaceAll("(?<=\")([a-zA-Z0-9\\.\\_]+)(\\.)([a-zA-Z0-9\\.\\_]+)(?=\")", "$1_$3")
    q = q.replace("\"","")
    q = q.replaceAll("\\((.*?)\\)::([^\\s]+)", "CAST($1 as $2)")
    q = q.replaceAll("([^\\s]*[^\\)])::([^\\s]+)", "CAST($1 as $2)")
    q = q.replaceAll("([^\\s\\(]+) >> (\\d+)", "shiftright($1, $2)")
    q = q.replaceAll("([^\\s\\(]+) << (\\d+)", "shiftleft($1, $2)")
    q = q.replaceAll("EVERY\\((.+?)\\)", "MIN($1)")
    q = q.replaceAll("SOME\\((.+?)\\)", "MAX($1)")
    q
  }

  //generate column names in JSON format
  def makecolumns(input: String): String = {
    var newinput = input.replaceAll("  ", " ")
    newinput = newinput.slice(2, newinput.length - 2)
    val cols = newinput.split(" , ")
    var output = ""
    for (col <- cols) {
      val splitted_col = col.split(" ")
      output = output+"\""+splitted_col(0)+"\": \"\", "
    }
    output.slice(0, output.length - 2)
  }

  // reorder columns to be same is the table defined in DD
  def reorder(input: String): ListBuffer[String] = {
    var newinput = input.replaceAll("  ", " ")
    newinput = newinput.slice(2, newinput.length - 2)
    val cols = newinput.split(" , ")
    var output = new ListBuffer[String]()
    for (col <- cols) {
      val splited_col = col.split(" ")
      output += splited_col(0)
    }
    output
  }

  def load_tables(string: String, spark:SparkSession, hadoop_dir:String):Unit ={
    val pattern = "(?s)(?<=FROM |JOIN )[^\\(].*?(?=GROUP|WHERE|UNION|INNER|OUTER|JOIN|\n* *ON|\\)|\n*$|\n\n+)".r
    val relations = pattern.findAllIn(string).toList.flatMap {
      m => m.replace("\"","").split(",\\n|\\n|\\s\\s+|, +|,").filter(_.nonEmpty)
    }.distinct
    for (relation <- relations){
      val relation_name = relation.split(" ")(0)
      println("/"+hadoop_dir+"/"+relation_name)
      val df = spark.read.load("/"+hadoop_dir+"/"+relation_name)
      df.createOrReplaceTempView(relation_name)
      println("load: /"+hadoop_dir+"/"+relation_name)
    }
  }
}
