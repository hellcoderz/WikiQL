import scala.math.random
import spark._
import SparkContext._
import scala.util.matching.Regex
import scala.collection.mutable.ListBuffer
import scala.io._
import scala.collection.mutable.HashMap
import scala.util.control._

object main {  

  //function to find min from an array
  def min(nums: Int*): Int = nums.min


//Levenshtein distance function for string similarity
  def similarity(str1: String, str2: String): Int = {
    val lenStr1 = str1.length
    val lenStr2 = str2.length
 
    val d: Array[Array[Int]] = Array.ofDim(lenStr1 + 1, lenStr2 + 1)
 
    for (i <- 0 to lenStr1) d(i)(0) = i
    for (j <- 0 to lenStr2) d(0)(j) = j
 
    for (i <- 1 to lenStr1; j <- 1 to lenStr2) {
      val cost = if (str1(i - 1) == str2(j-1)) 0 else 1
 
      d(i)(j) = min(
        d(i-1)(j  ) + 1,     // deletion
        d(i  )(j-1) + 1,     // insertion
        d(i-1)(j-1) + cost   // substitution
      )
    }
 
    d(lenStr1)(lenStr2)
  }
 
  
  //a function to extract data from a query for unstructured data
  //of the for "type:drug,kills|kill|against,bacteria"
  def map1(q:String):List[String] = {
    val spl = q.split(",")  //split the query in 3 parts namely arg1,rel,arg2
    var arg1 = spl(0).trim.toLowerCase
    var arg2 = spl(2).trim.toLowerCase
    var rel = spl(1).trim.toLowerCase
    var arg1_is_type = "0"
    var arg2_is_type = "0"

    if(rel contains "|")
      rel = rel.replace("|","---")

    if(arg1 != "-")
      arg1 = " " + arg1 + " "
    if(arg2 != "-")
      arg2 = " " + arg2 + " "

    if(arg1 contains "type:"){  //check if arg1 has type information or not
      arg1 = " "+arg1.split(":")(1).toUpperCase.trim+" "
      arg1_is_type = "1"
    }
    if(arg2 contains "type:"){  //check if arg2 has type information or not
      arg2 = " "+arg2.split(":")(1).toUpperCase.trim+" "
      arg2_is_type = "1"
    }
    //return a list of arg1,relation,arg1 and if arg1 has type info or not and also same for arg2
    List(arg1,rel,arg2,arg1_is_type,arg2_is_type)

  }

  //map function to search the query in the unstructured dataset with return type true or false for the filter command
  def map2(line:String,arg1:String,rel:String,arg2:String,a1_is_type:String,a2_is_type:String):Boolean = {
    val words = line.split("\t")  //split rhe words oth input line which is TAB separated
    //check id arg1 does not contains type info and also check if it is present in the input line
    if(a1_is_type == "0" && ((" "+words(1)+" ").toLowerCase contains arg1) || arg1 == "-"){
      //check id arg1 does not contains type info and also check if it is present in the input line
      if(a2_is_type == "0" && ((" "+words(3)+" ").toLowerCase contains arg2) || arg2 == "-"){
        if(rel contains "---"){   //check if relation contains more than on part
          val rels = rel.split("---")   //and split those parts
          //println("with or"+rels)
          for(r <- rels){   //iterate over those parts
            if(words(2) contains "---"){  //recursively check if there are more parts in the relation
              for(word <- words(2).split("---")){
                if((" "+word+" ").toLowerCase contains (" "+r+" ")){
                  return true
                }
              }
            }else{

                if((" "+words(2)+" ").toLowerCase contains (" "+r+" ")){
                  return true
                }
            }
          }
        }else{
         // println("no or"+rel)
          if(words(2) contains "---"){
              for(word <- words(2).split("---")){
                //println(word)
                if(((" "+word+" ").toLowerCase contains (" "+rel+" ")) || rel == "-"){
                  return true
                }
              }
          }else{
            if(((" "+words(2)+" ").toLowerCase contains (" "+rel+" ")) || rel == "-"){
              return true
            }
          }
        }
        //same as above but checking if arg2 has type information
      }else if(a2_is_type == "1" && (words(3) contains "<type=") && ((" "+words(3)+" ") contains arg2) || arg2 == "-"){
        if(rel contains "---"){
          val rels = rel.split("---")
          for(r <- rels){
            if(words(2) contains "---"){
              for(word <- words(2).split("---")){
                if((" "+word+" ").toLowerCase contains (" "+r+" ")){
                  return true
                }
              }
            }else{
                if((" "+words(2)+" ").toLowerCase contains (" "+r+" ")){
                  return true
                }
            }
          }
        }else{
          if(words(2) contains "---"){
              for(word <- words(2).split("---")){
                if(((" "+word+" ").toLowerCase contains (" "+rel+" ")) || rel == "-"){
                  return true
                }
              }
          }else{
            if(((" "+words(2)+" ").toLowerCase contains (" "+rel+" ")) || rel == "-"){
              return true
            }
          }
        }
      }
      //same as above but checking if arg1 has type information
    }else if(a1_is_type == "1" && (words(1) contains "<type=") && ((" "+words(1)+" ") contains arg1) || arg1 == "-"){
      //println(line)
      if(a2_is_type == "0" && ((" "+words(3)+" ").toLowerCase contains arg2) || arg2 == "-"){
        if(rel contains "---"){
          val rels = rel.split("---")
          for(r <- rels){
            if(words(2) contains "---"){
              for(word <- words(2).split("---")){
                if((" "+word+" ").toLowerCase contains (" "+r+" ")){
                  return true
                }
              }
            }else{
                if((" "+words(2)+" ").toLowerCase contains (" "+r+" ")){
                  return true
                }
            }
          }
        }else{
          if(words(2) contains "---"){
              for(word <- words(2).split("---")){
                if(((" "+word+" ").toLowerCase contains (" "+rel+" ")) || rel == "-"){
                  return true
                }
              }
          }else{
            if(((" "+words(2)+" ").toLowerCase contains (" "+rel+" ")) || rel == "-"){
              return true
            }
          }
        }
      }else if(a2_is_type == "1" && (words(3) contains "<type=") && ((" "+words(3)+" ") contains arg2) || arg2 == "-"){
        if(rel contains "---"){
          val rels = rel.split("---")
          for(r <- rels){
            if(words(2) contains "---"){
              for(word <- words(2).split("---")){
                if((" "+word+" ").toLowerCase contains (" "+r+" ")){
                  return true
                }
              }
            }else{
                if((" "+words(2)+" ").toLowerCase contains (" "+r+" ")){
                  return true
                }
            }
          }
        }else{
          if(words(2) contains "---"){  //checking if input relation part contains more parts
              for(word <- words(2).split("---")){
                if(((" "+word+" ").toLowerCase contains (" "+rel+" ")) || rel == "-"){
                  return true
                }
              }
          }else{
            if(((" "+words(2)+" ").toLowerCase contains (" "+rel+" ")) || rel == "-"){
              return true
            }
          }
        }
      }
    }
    return false
  }

//function to determine the best results among disambiguaous results
  def map3(values:Seq[String]):String = {
    if(values.length == 1)  //if there is no disambiguation or duplicate keys the return the first element in values list
      return values(0)

    val val_map = new HashMap[String,Int]   //create a hash map to store all test values
    for(v <- values){
        //println(v)
        val_map += v -> 0   //initialize the map with integer 1
    }
    var min = 9999
    var value = ""
    //find which of the values have the lowest added dissimilarity by comparing everyone to the other using similarity function
    for(v1 <- values){
      for(v2 <- values){
        val_map(v1) = val_map(v1) + similarity(v1,v2)
        if(val_map(v1) < min){
          min = val_map(v1)
          value = v1
        }
      }
    }
    (value)

  }

  //function to output (key,values) without the annotated information
  def map4(line:String):Tuple2[String,String] = {
    val words = line.split("\t")
    val pat = "<type=.*>(.*)</type>".r
    val word1 = pat.replaceAllIn(words(1),"$1").trim.toLowerCase
    val word3 = pat.replaceAllIn(words(3),"$1").trim

    (word1,word3)
  }
  //function to find the (arg1,agr2) if the query satifies the input for unstructured dataset
  def relation(query:String,input:RDD[String]):RDD[(String,String)] = {
    
    val query_string:String = query
    val query_lst:List[String] = map1(query_string)   //break the query
    val arg1:String = query_lst(0)  //set arg1
    val arg2:String = query_lst(2)  //set arg2
    val rel:String = query_lst(1)   //set relation
    val a1_is_type:String = query_lst(3)
    val a2_is_type:String = query_lst(4)
    val data = input.filter(line => map2(line,arg1,rel,arg2,a1_is_type,a2_is_type)).cache()   //filter out all those line from the input which satisfies the indivial query for arg1 , relation and arg2
    val data_m = data.map(line => map4(line)).cache()   //filter lines are not in key,values form so this step does that with key = arg1 and value = arg2
    val data_g = data_m.groupByKey().cache()  //then to find ambiguos results we group all the result based on the key
    val data_r = data_g.mapValues(values => map3(values)).cache()   //find those lines which have ambiguous values and remove it
    return data_r
  }


  //function to find the result based on type and categories if all of them are present
def findByTypeAndCategoryAND(line:String,tpe:String,category:String):Boolean = {
      val loop = new Breaks;
      var ccount = 0
      var tcount = 0
      val tq = tpe.split(":")
      val cq = category.split(":")
      val words = line.split("\t")
      for(i <- 1 to words.length - 1){
        val tcwords = words(i).split("<>")
        if(tcwords(0) == "category"){
          for(c <- 0 to cq.length - 1){
            loop.breakable{
              for(tc <- 1 to tcwords.length - 1){
                if((tcwords(tc).toUpperCase.trim).matches(".*"+cq(c).toUpperCase.trim+".*")){
                  ccount = ccount + 1
                  loop.break;

                }

              }
            }
          }
        }else if(tcwords(0) == "type"){
          for(t <- 0 to tq.length - 1){
            loop.breakable{
              for(tc <- 1 to tcwords.length - 1){
                if((tcwords(tc).toUpperCase.trim).matches(".*"+tq(t).toUpperCase.trim+".*")){
                  tcount = tcount + 1
                  loop.break;
                }

              }
            }
          }
        }else if(tcwords(0) == "occupation"){
          for(t <- 0 to tq.length - 1){
            loop.breakable{
              for(tc <- 1 to tcwords.length - 1){
                if((tcwords(tc).toUpperCase.trim).matches(".*"+tq(t).toUpperCase.trim+".*")){
                  tcount = tcount + 1
                  loop.break;
                }

              }
            }
          }
        }else if(tcwords(0) == "profession"){
          for(t <- 0 to tq.length - 1){
            loop.breakable{
              for(tc <- 1 to tcwords.length - 1){
                if((tcwords(tc).toUpperCase.trim).matches(".*"+tq(t).toUpperCase.trim+".*")){
                  tcount = tcount + 1
                  loop.break;
                }

              }
            }
          }
        }
      }

      if(tpe.trim == "-" && (ccount == cq.length)){
        return true
      }else if(category.trim == "-" && (tcount == tq.length)){
        return true
      }else if((ccount == cq.length) && (tcount == tq.length)){
        return true
      }

      return false
    
  }

  //function to find the result based on type and categories if only one of them are present
  def findByTypeAndCategoryOR(line:String,tpe:String,category:String):Boolean = {
      val tq = tpe.split(":")
      val cq = category.split(":")
      val words = line.split("\t")
      for(i <- 1 to words.length - 1){
        val tcwords = words(i).split("<>")
        if(tcwords(0) == "category"){
          for(c <- 0 to cq.length - 1){
              for(tc <- 1 to tcwords.length - 1){
                if((tcwords(tc).toUpperCase.trim).matches(".*"+cq(c).toUpperCase.trim+".*")){
                  return true

                }

              }
            
          }
        }else if(tcwords(0) == "type"){
          for(t <- 0 to tq.length - 1){
              for(tc <- 1 to tcwords.length - 1){
                if((tcwords(tc).toUpperCase.trim).matches(".*"+tq(t).toUpperCase.trim+".*")){
                  return true
                }

              }
            
          }
        }else if(tcwords(0) == "occupation"){
          for(t <- 0 to tq.length - 1){
              for(tc <- 1 to tcwords.length - 1){
                if((tcwords(tc).toUpperCase.trim).matches(".*"+tq(t).toUpperCase.trim+".*")){
                  return true
                }

              }
            
          }
        }else if(tcwords(0) == "profession"){
          for(t <- 0 to tq.length - 1){
              for(tc <- 1 to tcwords.length - 1){
                if((tcwords(tc).toUpperCase.trim).matches(".*"+tq(t).toUpperCase.trim+".*")){
                  return true
                }

              }
            
          }
        }
      }

      return false
  }

  //function to find the result based on keys other than type and categories if all of them are present
  def findOtherAND(line:String,key:String,value:String):Boolean = {
    val words = line.split("\t")
    for(i <- 1 to words.length - 1){
      val tcwords = words(i).split("<>")
      if(tcwords(0).trim == key.toLowerCase.trim){
        for(k <- 1 to tcwords.length - 1){
          if((tcwords(k).toLowerCase.trim).matches(value.toLowerCase.trim)){
            return true
          }
        }
      }
    }
    return false
  }

  //function to find the result based on keys other than type and categories if only one of them are present
  def findOtherOR(line:String,key:String,value:String):Boolean = {
    val words = line.split("\t")
    for(i <- 1 to words.length - 1){
      val tcwords = words(i).split("<>")
      if(tcwords(0).trim == key.toLowerCase.trim){
        for(k <- 1 to tcwords.length - 1){
          if(tcwords(k).toLowerCase.trim == value.toLowerCase.trim){
            return true
          }
        }
      }
    }
    return false
  }
  //just create a key value pair
  def mapstokey(line:String):Tuple2[String,String] = {
    val words = line.split("\t")
    (words(0).trim.toLowerCase,"key")
  }

  def main(args: Array[String]) {
    if (args.length == 0) {
      System.err.println("Usage: query input_path query_string <master>")
      System.exit(1)
    }
    val sc = new SparkContext(args(0), "query",  System.getenv("SPARK_HOME"), List(System.getenv("SPARK_TEST")))  //spark context for parralel computations
    val inputUS = sc.textFile("../data/reverbed.txt")   //unstrucutred input file
    val input = sc.textFile("../data/final.txt")  //structured input file
