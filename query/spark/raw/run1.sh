echo "Compiling query.."
fsc -cp spark.jar query1.scala
echo "Running query.."
scala -cp spark.jar query1 local
