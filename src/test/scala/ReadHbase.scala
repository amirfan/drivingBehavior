import com.sssjd.unicredit.spark.hbase._
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.hadoop.conf.Configuration
object ReadHbase {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("s").master("local[*]").getOrCreate()

    val hbaseConf: Configuration = HBaseConfiguration.create()
    val rootdir = "hdfs://192.168.100.183:8020/hbase"
    val quorum = "192.168.100.11,192.168.100.177,192.168.100.204"
    hbaseConf.set("hbase.rootdir",rootdir)
    hbaseConf.set("hbase.zookeeper.quorum",quorum)
//    hbaseConf.set("hbase.zookeeper.property.clientPort","2181")

    @transient implicit val config: HBaseConfig = HBaseConfig(hbaseConf)
    val sc = spark.sparkContext

    sc.hbase[String]("taxi_ns:rapidDetails",Set("cf")).foreach{
      case (key,vals) =>
        println(key)
        println("--------------")
    }

  }

}
