echo "Compiling query.."
fsc -cp spark.jar query.scala
echo "Running query.."
scala -cp spark.jar query local
