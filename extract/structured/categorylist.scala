import scala.math.random
import spark._
import SparkContext._
import scala.util.matching.Regex

object categorylist {
  def map1(line:String):(String,String) = {
    
    // variable to store individual category as key 

    var key = new String
    
    try{

    // Splits line 
    
    val arr = line.split(" ")
    
    if(arr.length>1)
    {
      
      // extract individual category 

      var si=arr(2).lastIndexOf(':')
      var ei=arr(2).indexOf('>')
      key = arr(2).substring(si+1,ei).replaceAll("_"," ")

    }

    // replace special charachters
    
    key = key.replaceAll(",",";")

    }
    catch{

    case e: Exception => println("**** "+ line  + "****")
          
         }

    // return key,and empty value

    (key,"")
  }


  def main(args: Array[String]) {
    if (args.length == 0) {
      System.err.println("Usage: SparkPi <master> [<slices>]")
      System.exit(1)
    }
    
    val spark = new SparkContext(args(0), "Category List",System.getenv("SPARK_HOME"),Seq(System.getenv("SPARK_JAR")))
    val slices = 2
    val file=spark.textFile(args(1))

    // filter invalid lines

    val newfile = file.filter( line => !(line.charAt(0) == '#'))
    
    // call map fuction with each line

    val mout=newfile.map(
      line => map1(line)
       ).cache()

    // perform reduction on each of key,value pair 
    
    val rout = mout.reduceByKey(_+_)

    // save the resultant list of categories as key,and empty "" as value to specified file
      
    rout.saveAsTextFile(args(2));
    System.exit(0)
}
}