import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

val conf= new SparkConf().setMaster("local").setAppName("test")
val sc = new SparkContext(conf)
sc.setLogLevel("WARN")

val rdd1= sc.parallelize(Array(1,2,3,4,5))
println("elements of rdd1")
rdd1.foreach(x=>print(x+","))
println()

val rdd2 = sc.parallelize(List(6,7,8,9,10))
println("elements of rdd2")
rdd2.foreach(x=>print(x+","))
println()

val rdd3=rdd1.union(rdd2)
println("elements of rdd3")
rdd3.foreach(x=>print(x+","))
