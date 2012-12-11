import scala.math.random
import spark._
import SparkContext._
import scala.util.matching.Regex

object person {
  def map1(line:String):(String,String) = {
    
    // Regular expression for matching Person name, surname, givenName

    val category1 = new Regex("foaf/0.1/(\\w+)")
    
    // Regular expression for matching Person description

    val category2 = new Regex("elements/1.1/(\\w+)")
    
    // Regular expression for matching Person type

    val category3 = new Regex("#(\\w+)>")
    
    // Regular expression for matching all other relevant information of person

    val category4 = new Regex("ontology/(\\w+)>")

    // variable to store Person name as key, and all its detail as value

    var key = new String
    var value = new String
    
    var val1 = new String
    var val2 = new String
    var val3 = new String
    var val4 = new String    
    
    try{
    
    // Splits line 

    val arr = line.split(" ")

    if(!arr(0).equals(""))
    {

      // extract key i.e., titile of article(Person name)

      var si=arr(0).indexOf("resource/")
      var ei=arr(0).lastIndexOf('>')
      key = arr(0).substring(si+9,ei).replaceAll("_"," ")
     
    }

    // Pattern matching

    (category1 findAllIn line).matchData foreach(m => val1=(m.subgroups mkString ""))
    (category2 findAllIn line).matchData foreach(m => val2=(m.subgroups mkString ""))
    (category3 findAllIn line).matchData foreach(m => val3=(m.subgroups mkString ""))
    (category4 findAllIn line).matchData foreach(m => val4=(m.subgroups mkString ""))
    
    // For extracting name,givenName and surname

    if(val1.toString.equals("name")||val1.toString.equals("surname")||val1.toString.equals("givenName"))
    {

      val si=line.indexOf('"')
      val ei=line.lastIndexOf('"')
      val temp= line.substring(si+1,ei)
      value= val1 + ":"+temp

    }

    // For extracting description of person 
    
    else
    if(val2.toString.equals("description"))
    { 
      val si=line.indexOf('"')
      val ei=line.lastIndexOf('"')
      val temp= line.substring(si+1,ei)
      value= val2+":"+ temp
    }

    // For extracting type of person

    else
    if(val3.toString.equals("type"))
    {
      value= val3+":"+val1
    } 
    
    // For extracting birthPlace and deathPlace
    
    else
    if(val4.toString.equals("birthPlace")||val4.toString.equals("deathPlace"))
    {
      val si=arr(2).lastIndexOf('/')
      val ei=arr(2).indexOf('>')
      value = val4+":"+arr(2).substring(si+1,ei)
    }
    
    // For extracting birthDate and deathDate

    else
    if(val4.toString.equals("birthDate")||val4.toString.equals("deathDate"))
    {
      val si=arr(2).indexOf('"')
      val ei=arr(2).indexOf('"',si+1)
      value = val4+":"+arr(2).substring(si+1,ei)
    }

    // replace special charachters

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
    val spark = new SparkContext(args(0), "Person information",System.getenv("SPARK_HOME"),Seq(System.getenv("SPARK_JAR")))
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