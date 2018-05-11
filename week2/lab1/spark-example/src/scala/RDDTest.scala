import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object RDDTest {
  def main(args: Array[String]) {
    val master = "local";
    val conf = new SparkConf().setAppName("Spark RDD example").setMaster(master)
    val sc = new SparkContext(conf);
    val a = sc.textFile("hdfs://localhost:9000/user/hadoop/studenttab10k");
    val b = a.map(line => line.split("\t"));
    val c = b.filter(t => Integer.parseInt(t(1))>18);
    val d = c.map(t =>(t(0), math.round(t(2).toDouble)));
    val e = d.aggregateByKey((0.0,0))((acc, value) =>(acc._1 + value, acc._2 +1), (acc1, acc2) =>(acc1._1 + acc2._1, acc1._2 + acc2._2));
    val f = e.mapValues(sumCount =>1.0* sumCount._1 / sumCount._2);
    val g = f.distinct;
    g.saveAsTextFile("output");
  }
}