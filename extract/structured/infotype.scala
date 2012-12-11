import scala.math.random
import spark._
import SparkContext._
import scala.util.matching.Regex

object infotype {
  def map1(line:String):(String,String) = {
    
     // variable to store each type of an article    
    
    var key = new String
    var value = new String
     
         
    try {
    
    // Splits line 

    var arr= line.split(" ")

    // Check if line has above array arr has 3 elements

    if(arr.length>1)
    {

      // extract key i.e., titile of article

      var si=arr(0).indexOf("resource/")
      var ei=arr(0).lastIndexOf('>')
      key = arr(0).substring(si+9,ei).replaceAll("_"," ")

      // Check for ontology tag and extract all type

      if(arr(2).indexOf("ontology")>0)
      {
      si=arr(2).lastIndexOf('/')
      ei=arr(2).indexOf('>')
      value = "type:"+arr(2).substring(si+1,ei).replaceAll("_"," ")
      }

    }

    // replace special charachters

    key = key.replaceAll(",",";")
    value = value.replaceAll(",",";")

   }
  
  catch{
    
    case e: Exception => println("**** "+ line  + "****")
      
        }

     // return key,value

    (key,value)
  }


  def main(args: Array[String]) {
    if (args.length == 0) {
      System.err.println("Usage: SparkPi <master> [<slices>]")
      System.exit(1)
    }
    val spark = new SparkContext(args(0), "Type List",System.getenv("SPARK_HOME"),Seq(System.getenv("SPARK_JAR")))
    val slices = 2
    val file=spark.textFile(args(1))
    
    // filter invalid lines
    
    val newfile = file.filter(line => !(line.charAt(0)=='#'))

    val mout=newfile.map(
      line => map1(line)
       ).cache()

    // perform reduction on each of key,value pair

    val rout = mout.reduceByKey(_+"\t"_)

    // save the resultant key,value pair to specified file
      
    rout.saveAsTextFile(args(2));
    System.exit(0)
}
}