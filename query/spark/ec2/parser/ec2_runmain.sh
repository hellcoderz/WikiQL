export SCALA_HOME=/root/scala-2.9.2
echo "COMPILING"
/root/scala-2.9.2/bin/fsc -cp spark.jar main.scala
echo "CREATING JAR"
jar -cf "wikiql.jar" *.class
export SPARK_TEST=`pwd`/wikiql.jar
echo "RUNNING"
../../run main localhost:5050 > /dev/null 2>&1
echo "DONE"
rm -rf out
mkdir out
/root/ephemeral-hdfs/bin/hadoop dfs -get /cloud/out/part-* /root/spark/wikiql/ec2_main/out
cd out
cat part-* > ../result.out
