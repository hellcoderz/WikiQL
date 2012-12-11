import scala.math.random
import spark._
import SparkContext._
import scala.util.matching.Regex

object finalkeyvalue {
  def map1(line:String):(String,String) = {
   
    // variable to store all values associated with an article 
    var key = new String
    var value = new String
    
    try{

      // extract keys and values from each line

      var ei=line.indexOf(",")
      if(ei>0)
      {
        key = line.substring(1,ei).replaceAll("_"," ")
        value = line.substring(ei+1,line.length()-1).replaceAll("_"," ")
      }
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
    val spark = new SparkContext(args(0), "Category",System.getenv("SPARK_HOME"),Seq(System.getenv("SPARK_JAR")))
    val slices = 2
    
    val file1=spark.textFile("out_1")     // input file from Category
    val mout1=file1.map(
      line => map1(line)
       )

    val file2=spark.textFile("out_2")     // input file  from infobox property
    val mout2=file2.map(
      line => map1(line)
       )

    val file3=spark.textFile("out_3")     // input file  from  specific file
    val mout3=file3.map(
      line => map1(line)

       )

    val file4=spark.textFile("out_4")     // input file from type of article
    val mout4=file4.map(
      line => map1(line)
       )

    val file5=spark.textFile("out_5")     // input file from person info
    val mout5=file5.map(
      line => map1(line)
       )

    // Perform union of all the key value pairs  from respective file and reduce by key

    val out = mout1.union(mout2).union(mout3).union(mout4).union(mout5).reduceByKey(_+"\t"+_)

    // save the resultant key,value pair to specified file
      
    out.saveAsTextFile(args(1));
    System.exit(0)
}
}