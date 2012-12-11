import scala.math.random
import spark._
import scala.io.Source
import SparkContext._
import scala.util.matching.Regex

object category {
  def map1(line:String):(String) = {
    
    
  

  
 val pattern5 = new Regex("(.)(0[1-9]|[12][0-9]|3[01])(\\ )(January|February|March|April|May|June|July|August|September|October|November|December)(\\ )(19|20)(\\d\\d)(.)");
       val pattern4 = new Regex("((January|February|March|April|May|June|July|August|September|October|November|December)(\\ )*(\\t)*(1|2|3|4|5|6|7|8|9)*(0[1-9]|[12][0-9]|3[01])(\\ )*(,)*(\\ )*(\\t)*(19|20)(\\d\\d)(\\n)*)");
   val pattern = new Regex("(0[1-9]|1[012])([- /.])(0[1-9]|[12][0-9]|3[01])([- /.])(19|20)(\\d\\d)")
      //email
      val pattern2 = new Regex("([A-Za-z0-9])(([_\\.\\-]?[a-zA-Z0-9]+){0,15})(@)([A-Za-z0-9]+)(([\\.\\-]?[a-zA-Z0-9]+){0,14})(\\.)([A-Za-z]{2,})")
    //time
      val pattern3 = new Regex("(20|21|22|23|[01]\\d|\\d)(([:][0-5]\\d){1,2})")
      //url
    //  val pattern4 = new Regex("(https?:\\/\\/)?([\\da-z\\.-]+)\\.([a-z\\.]{2,6})([\\/\\w \\.-]*)*\\/?");
      //distance
      val pattern6 = new Regex("(([0-9]){1,3}(,)*([0-9]){0,4}(,)*([0-9]){0,4}(\\ )*(million|billion|thousand|hundred thousand)*(\\ )*(meter|ft|feet|foot|miles|mile|kilometer|m)+)(\\W)") 
      //area
      val pattern7 = new Regex("(([0-9]){1,4}(,)*([0-9]){0,4}(,)*([0-9]){0,4}(\\ )*(million|billion|thousand|hundred thousand)*(\\ )*(sq kilometer|square kilometer|sq ft|m2)+)") 
   //volume
      val pattern8 = new Regex("(([0-9]){0,4}(,)*([0-9]){0,4}(,)*([0-9]){0,4}(\\ )*(million|billion|thousand|hundred thousand)*(\\ )*(oz|cubic meter|cc|ounce|fl. oz|fl oz)+)") 
   // currency
      val pattern9 = new Regex("(([0-9]){0,4}(,)*([0-9]){0,4}(,)*([0-9]){0,4}(\\ )*(million|billion|thousand|hundred thousand)*(\\ )*(Dollars|\\$)+)") 
      val pattern10 = new Regex("((US)*(\\$)(\\ )*(dollar)*([0-9]){1,4}(,)*([0-9]){0,4}(,)*([0-9]){0,4}(\\ )*(million|billion|thousand|hundred thousand)*(\\ )*)") 
 
  var str = line;

str= pattern replaceAllIn(str,"<type= DATE >"+"$1$2$3$4$5$6"+"</type>")
   // str=pattern2 replaceAllIn(str,"<type= EMAIL-ID>"+"$1$2$4$5$6$8$9"+"</type>")
    str=pattern3 replaceAllIn(str,"<type= TIME >"+"$1$2"+"</type  >")
str= pattern4 replaceAllIn(str,"<type= DATE >"+"$1"+"</type>")
str= pattern5 replaceAllIn(str,"<type= DATE >"+"$1$2$3$4$5$6$7$8"+"</type>")
str=pattern6 replaceAllIn(str,"<type= DISTANCE >"+"$1"+"</type>")
str=pattern7 replaceAllIn(str,"<type= AREA >"+"$1"+"</type>")
str=pattern8 replaceAllIn(str,"<type= VOLUME >"+"$1"+"</type>")
str=pattern9 replaceAllIn(str,"<type= CURRENCY >"+"$1"+"</type>")
str = pattern10 replaceAllIn(str,"<type= CURRENCY >"+"$1"+"</type>")



   

   (str)
  }


  def main(args: Array[String]) {
    if (args.length == 0) {
      System.err.println("Usage: SparkPi <master> [<slices>]")
      System.exit(1)
    }
    val spark = new SparkContext(args(0), "Category",System.getenv("SPARK_HOME"),Seq(System.getenv("SPARK_JAR")))
    val slices = 2
    val file=spark.textFile(args(1))

    val mout=file.map(
      line => map1(line)
       ).cache()
 //   val rout = mout.reduceByKey(_+","+_)
      
    mout.saveAsTextFile("out_2");
    System.exit(0)
}
}
