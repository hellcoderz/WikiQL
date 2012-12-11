import scala.math.random
import spark._
import SparkContext._
import scala.util.matching.Regex
import scala.collection.mutable.ListBuffer
import scala.io._

object annotate {

  def map1(line:String){

    val word:String = (line.trim().toLowerCase())
    word
  }

  def map3(line:String,names:spark.broadcast.Broadcast[List[String]],annotate_text:String):String = {
    val words = line.split("\t")
    var l = "<-->"
    var word1 = ""
    var word2 = ""
    var flag = 0
    for(i <- 0 to (names.value.length - 1)){
     // println("words(1)->"+(" " + words(1).trim.toLowerCase +" ")+"|")
     // println("words(3)->"+(" " + words(3).trim.toLowerCase +" ")+"|")
     // println("name->"+(" "+names.value(i).trim.toLowerCase+" ")+"|")
      if((" " + words(1).trim.toLowerCase +" ") contains (" "+names.value(i).trim.toLowerCase+" ")) {
        l = line
        if(words(1).trim.toLowerCase contains "</type>"){
          val pat = "<type=(\\w+)>.+</type>".r
          var type1:String = ""
          (pat findAllIn words(1)).matchData foreach{ m =>
            type1 = m.group(1)
          }
          val annotated_line = (words(1)).replaceAll("<type="+type1+">","<type="+annotate_text.toUpperCase+"|"+type1.toUpperCase+">")
          words(1) = annotated_line.trim
          flag = 1
        }else{
          val annotated_line = (" " + words(1).trim.toLowerCase +" ").replaceAll(" "+names.value(i).trim.toLowerCase+" "," <type="+annotate_text.toUpperCase+"> "+names.value(i)+" </type> ")
          words(1) = annotated_line.trim
          flag = 1
        }
      }
    }


    for(i <- 0 to (names.value.length - 1)){
      if((" " + words(3).trim.toLowerCase +" ") contains (" "+names.value(i).trim.toLowerCase+" ")){
        l = line
        if(words(3).trim.toLowerCase contains "</type>"){
          val pat = "<type=(\\w+)>.+</type>".r
          var type1:String = ""
          (pat findAllIn words(3)).matchData foreach{ m =>
            type1 = m.group(1)
          }
          val annotated_line = (words(3)).replaceAll("<type="+type1+">","<type="+annotate_text.toUpperCase+"|"+type1.toUpperCase+">")
          words(3) = annotated_line.trim
          flag = 1
        }else{
          val annotated_line = (" " + words(3).trim.toLowerCase +" ").replaceAll(" "+names.value(i).trim.toLowerCase+" "," <type="+annotate_text.toUpperCase+"> "+names.value(i)+" </type> ")
          words(3) = annotated_line.trim
          flag = 1
        }
      }
    }


    if(flag == 0){
      return line
      }else{
        return words(0)+"\t"+words(1)+"\t"+words(2)+"\t"+words(3)
      }
    
  }

  

  def main(args: Array[String]) {
    if (args.length == 0) {
      System.err.println("Usage: annotate annotate_input_file annotate_use_file annotate_text <master>")
      System.exit(1)
    }
    val spark = new SparkContext(args(4), "annotate",  System.getenv("SPARK_HOME"), List(System.getenv("SPARK_TEST")))
    val store = Source.fromFile(args(0))
    val data = spark.textFile(args(1))
    val annotate_text = args(2)

    var names:List[String] = Nil

    //println(args(0))
    //println(args(1))
    //println(args(2))
    //println(args(3))
    store.getLines.foreach((line) => {names = names:::List(line.trim)})

    val names_data = spark.broadcast(names)

    val data_set = data.map(line => map3(line,names_data,annotate_text))
    //val clean_data_set = data_set.filter(line => if(line.contains("<-->")) false else true).cache()

    //val st = store.flatMap(line =>  map1(line))

    //val dt = data.flatMap(line => map2(line,st))
    data_set.saveAsTextFile(args(3))
    System.exit(0)
  }
}