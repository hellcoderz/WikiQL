import scala.math.random
import spark._
import SparkContext._
import scala.util.matching.Regex

object infospecific {
  def map1(line:String):(String,String) = {
    
    // variable to store specific property associated with a key         
    
    var key = new String
    var value = new String

    try{ 
    
    // Splits line 

    val arr = line.split(" ")

     
    if(arr.length>2)
    { 

      // extract article title as key 

      var si=arr(0).lastIndexOf("resource/")
      var ei=arr(0).lastIndexOf('>')
      key = arr(0).substring(si+9,ei)

      // extract name of property

      si=arr(1).lastIndexOf('/')
      ei=arr(1).indexOf('>')
      val temp1= arr(1).substring(si+1,ei)

      // extract value for above extracted property

      si=arr(2).indexOf('"')
      ei=arr(2).lastIndexOf('"')
      val temp2= arr(2).substring(si+1,ei)

      // extract datatype of above extracted value

      si=arr(2).lastIndexOf('/')
      ei=arr(2).indexOf('>')
      val temp3= arr(2).substring(si+1,ei)

      value= temp1 + ":" + temp2 + " datatype:"+ temp3 
    }

    // replace special charachters

    key = key.replaceAll("_"," ")
    value = value.replaceAll("_"," ") 
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
    val spark = new SparkContext(args(0), "Specific property",System.getenv("SPARK_HOME"),Seq(System.getenv("SPARK_JAR")))
    val slices =  2
    val file=spark.textFile(args(1))

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