import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object DataFrameTest {
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

        df.where("age > 18")
          .selectExpr("age", "round(gpa) as gpa")
          .groupBy("age")
          .avg("gpa")
          .show()
    }
}
