import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object ExploratoryAnalysis extends App {
  var conf = new SparkConf()
    .setAppName("prakash")
    .setMaster("local[5]")

  val sc = SparkContext.getOrCreate(conf)

  val spark = SparkSession.builder()
    .appName("Inclass ModelGenerator")
    .config("spark.sql.warehouse.dir", "D://BigData Spark//spark-warehouse")
    .getOrCreate()

  import spark.implicits._

 // val raw = spark.read.text("data-files")
    val raw = spark.read.option("header",true).option("inferSchema",true).csv("data-files/train.csv")
  
    //raw.describe().show
     raw.show()
    
  
  import org.apache.spark.ml.feature.QuantileDiscretizer
   
    val discretizer_t = new QuantileDiscretizer().setInputCol("atemp").setOutputCol("atempbin").setNumBuckets(2)
    
    val df_t = discretizer_t.fit(raw).transform(raw).drop("atemp")
    
    df_t.show()
    
    val discretizer_w = new QuantileDiscretizer().setInputCol("windspeed").setOutputCol("windspeedbin").setNumBuckets(4)
    val df_w = discretizer_w.fit(df_t).transform(df_t).drop("windspeed")
    
  import org.apache.spark.sql.types.DoubleType
  
    val discretizer_h = new QuantileDiscretizer().setInputCol("humidity").setOutputCol("humiditybin").setNumBuckets(4)
    val casted = df_w.withColumn("humidity", $"humidity".cast(DoubleType))
    val df = discretizer_h.fit(casted).transform(casted).drop("humidity")
    
    //df.show()
    


}