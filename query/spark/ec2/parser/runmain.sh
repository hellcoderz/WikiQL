echo "COMPILING"
fsc -cp spark.jar main.scala
echo "CREATING JAR"
jar -cf "wikiql.jar" *.class
export SPARK_TEST=`pwd`/wikiql.jar
echo "RUNNING"
scala -cp spark.jar main local > /dev/null 2>&1
echo "DONE"
