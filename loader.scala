  def relation(spark:SparkContext,query:String,input_file:String,output_file:String){
    val input = spark.textFile(input_file)
    val query_string:String = query
    val query_lst:List[String] = map1(query_string)
    val arg1:String = query_lst(0)
    val arg2:String = query_lst(2)
    val rel:String = query_lst(1)
    val a1_is_type:String = query_lst(3)
    val a2_is_type:String = query_lst(4)
    println(arg1)
    println(rel)
    println(arg2)
    val data = input.filter(line => map2(line,arg1,rel,arg2,a1_is_type,a2_is_type)).cache()
    val data_m = data.map(line => {
        val words = line.split("\t")
        (words(1).trim.toLowerCase,words(3).trim)
      }).cache()
    val data_g = data_m.groupByKey().cache()
    val data_r = data_g.mapValues(values => map3(values))
    //println(data_r)
   data_r.saveAsTextFile(output_file)
   //return data_r
  }

  relation(sc,"shahrukh khan,born on,-","reverbed.txt","out")

