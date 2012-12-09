import scala.util.control._

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
                if(tcwords(tc).toUpperCase.trim == cq(c).toUpperCase.trim){
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
                if(tcwords(tc).toUpperCase.trim == tq(t).toUpperCase.trim){
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


    def findByTypeAndCategoryOR(line:String,tpe:String,category:String):Boolean = {
      val tq = tpe.split(":")
      val cq = category.split(":")
      val words = line.split("\t")
      for(i <- 1 to words.length - 1){
        val tcwords = words(i).split("<>")
        if(tcwords(0) == "category"){
          for(c <- 0 to cq.length - 1){
              for(tc <- 1 to tcwords.length - 1){
                if(tcwords(tc).toUpperCase.trim == cq(c).toUpperCase.trim){
                  return true

                }

              }
            
          }
        }else if(tcwords(0) == "type"){
          for(t <- 0 to tq.length - 1){
              for(tc <- 1 to tcwords.length - 1){
                if(tcwords(tc).toUpperCase.trim == tq(t).toUpperCase.trim){
                  return true
                }

              }
            
          }
        }
      }

      return false
  }



    def findOtherAND(line:String,key:String,value:String):Boolean = {
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