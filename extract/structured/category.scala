import scala.math.random
import spark._
import SparkContext._
import scala.util.matching.Regex

object category {
  def map1(line:String):(String,String) = {
    
    // variable to store article as key and its category as value

    var key = new String
    var value = new String
    
    try{
    
    // Splits line 

    val arr = line.split(" ")

    if(arr.length>1)
      {
        // check for the tag "resource" and its index
        
        var si = arr(0).indexOf("resource/")
        var ei = arr(0).lastIndexOf('>')

        // extract key i.e., titile of article
        key = arr(0).substring(si+9,ei).replaceAll("_"," ")

        si = arr(2).lastIndexOf('/')
        ei = arr(2).indexOf('>')
        
        // extract corresponding category 

        value = arr(2).substring(si+1,ei).replaceAll("_"," ")
      }
    // replace special charachters

    key = key.replaceAll(",",";")
    value = value.replaceAll(",",";")
    
      }
    catch{
    
    case e: Exception => println("**** Error on Line : "+ line  + "****")

         }

    // return key,value
    (key,value)
  }


  def main(args: Array[String]) {
    if (args.length == 0) {
      System.err.println("Usage: SparkPi <master> [<slices>]")
      System.exit(1)
    }
    
    val spark = new SparkContext(args(0), "Category",System.getenv("SPARK_HOME"),Seq(System.getenv("SPARK_JAR")))
    val slices = 2
    val file = spark.textFile(args(1))
    
    // filter invalid lines

    val newfile = file.filter( line => !(line.charAt(0) == '#'))
    
    // call map fuction with each line

    val mout=newfile.map(
      line => map1(line)
       ).cache()

    // perform reduction on each of key,value pair 

    val rout = mout.reduceByKey(_+"\t"+_)
    
    // save the resultant key,value pair to specified file

    rout.saveAsTextFile(args(2));
    
    System.exit(0)
}
}