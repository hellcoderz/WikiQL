import scala.math.random
import spark._
import SparkContext._
import scala.util.matching.Regex

object infoproperty {
  def map1(line:String):(String,String) = {

    // Regular expression for matching name of person and homepage

    val name = new Regex("foaf/0.1/(\\w+)")
       
    // variable to store article as key and its property as value

    var key = new String
    var value = new String
    var Name = new String

    try {

    // Splits line 

    val arr = line.split(" ")

    if(arr.length>1)
    {
      // extract article title as key 

      var si=arr(0).indexOf("resource/")
      var ei=arr(0).lastIndexOf('>')
      key = arr(0).substring(si+9,ei).replaceAll("_"," ")
     
    }
    
    // Pattern matching

    (name findAllIn line).matchData foreach(m => Name=(m.subgroups mkString ""))  
    
    
    if(Name.toString.equals("name"))            // extract Name
    {
      var si=line.indexOf('"')
      var ei=line.lastIndexOf('"')
      var temp= line.substring(si+1,ei)
      value= Name+":"+temp
    }
    
    else
    if(Name.toString.equals("homepage"))        // extract homepage
    {
      value = Name + ":"+ arr(2)
    }

    else
    if(arr(1).indexOf('#')>=0)                  // extract other deatils like Type,Lat,Long
    {
      var si=arr(1).indexOf('#')
      var ei=arr(1).indexOf('>',si+1)
      var temp = arr(1).substring(si+1,ei)
      if(line.indexOf('"')>0)
      {
        si = line.indexOf('"')
        ei = line.lastIndexOf('"')
        value = temp +":"+ line.substring(si+1,ei)
      }
      else
      {
        si=arr(2).lastIndexOf('/')
        ei=arr(2).indexOf('>')
        value = temp +":"+arr(2).substring(si+1,ei)
      }
    }

    else                                        // extract Ontology
    {
    var si=arr(1).lastIndexOf('/')
    var ei=arr(1).indexOf('>')
    var ontology = arr(1).substring(si+1,ei)

    if(arr(2).indexOf("resource")>0)              // With resource tag
      { 
        si=arr(2).lastIndexOf('/')
        ei=arr(2).indexOf('>')
        var temp= arr(2).substring(si+1,ei)
        value= ontology+":"+ temp
      }
    else                                          // With some string
      { 
        si=line.indexOf('"')
        ei=line.lastIndexOf('"')
        var temp= line.substring(si+1,ei)
        value= ontology+":"+ temp
      }
    }
    
    // replace special charachters
 
    value=value.replaceAll("_"," ")
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
    val spark = new SparkContext(args(0), "Infobox property",System.getenv("SPARK_HOME"),Seq(System.getenv("SPARK_JAR")))
    val slices = 2
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