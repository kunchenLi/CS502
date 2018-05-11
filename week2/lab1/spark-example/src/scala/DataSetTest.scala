import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

case class Student(name:String, age:Int, gpa:Double)

object DataSetTest {
    def main(args: Array[String]) {

        val spark = SparkSession.builder()
                                .appName("Spark DataFrame example")
                                .getOrCreate()

        val studentSchema = StructType(Array(
            StructField("name", StringType, true),
            StructField("age", IntegerType, true),
            StructField("gpa", DoubleType, true)))

        var df = spark.read.option("delimiter", "\t")
            .schema(studentSchema)
            .csv("hdfs://localhost:9000/user/hadoop/studenttab10k")

        import spark.implicits._
        val ds = df.as[Student]
        ds.filter(s => s.age>18)
          .map(s => (s.age, math.round(s.gpa)))
          .groupByKey(s => s._1)
          .agg(avg($"_2").as[Double])
          .show()
    }
}