//val input = sc.textFile("../data/test.txt")
val input = sc.textFile("../data/final.txt")
val out2 = input.filter(line => findByTypeAndCategoryOR(line,"Actor:Adultactor","-") )
val out1 = out2.filter(line => findByTypeAndCategoryAND(line,"-","1967 births:Living people") )
val out = out1.filter(line => findOtherAND(line,"birthplace","Czech Republic"))