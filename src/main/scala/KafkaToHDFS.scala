
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.types.{StringType, StructType, TimestampType}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}
import scopt.OptionParser
import org.apache.log4j.{Level, Logger}





object KafkaToHDFS{


  case class Params(kafkaBrokers : String,
                          kafkaGroupID : String,
                          kafkaTopics : String,
                          appName : String,
                          hdfsPath : String,
                          duration : String)




  def main(args: Array[String]) {

    val parser = new OptionParser[Params]("scopt") {
      opt[String]('b', "kafkabrokers").action((x, c) => c.copy(kafkaBrokers = x)).text("Comma Seperated Kafka Brokers")
      opt[String]('g', "kafkagroupid").action((x, c) => c.copy(kafkaGroupID = x)).text("Kafka Group ID")
      opt[String]('t', "kafkatopics").action((x, c) => c.copy(kafkaTopics = x)).text("Comma Seperated Kafka Topics")
      opt[String]('n', "appname").action((x, c) => c.copy(appName = x)).text("Comma Seperated Kafka Topics")
      opt[String]('p', "hdfspath").action((x, c) => c.copy(hdfsPath = x)).text("Output Path Of HDFS Files")
      opt[String]('d', "duration").action((x, c) => c.copy(duration = x)).text("Streaming Batch Duration")


    }

    parser.parse(args, Params("", "", "", "", "", "")) match {
      case Some(config) => {
        // do stuff
        println("App Name : " + config.appName)
        println("Kafka Broker :" + config.kafkaBrokers)
        println("Kafka Group ID : " + config.kafkaGroupID)
        println("Kafka Topics : " + config.kafkaTopics)
        println("HDFS Path  : " + config.hdfsPath)
        println("Batch Duration  : " + config.duration)

        run(config)
      }
      case None =>

        println("Bad Arguments")
      // arguments are bad, error message will have been displayed
    }





    def run(params: Params) {

      val spark = SparkSession
        .builder()
        .appName(params.appName)
        .getOrCreate()


      val ssc = new StreamingContext(spark.sparkContext, Seconds(params.duration.toInt))

     //val rootLogger = Logger.getRootLogger()
      //rootLogger.setLevel(Level.ERROR)


      val kafkaParams = Map[String, Object](
        "bootstrap.servers" -> params.kafkaBrokers,
        "key.deserializer" -> classOf[StringDeserializer],
        "value.deserializer" -> classOf[StringDeserializer],
        "group.id" -> params.kafkaGroupID,
        "auto.offset.reset" -> "latest"
      )


      val topics = Array(params.kafkaTopics)

      val stream = KafkaUtils.createDirectStream[String, String](
        ssc,
        PreferConsistent,
        Subscribe[String, String](topics, kafkaParams)
      )


      val schema = new StructType()
        .add("date", StringType)
        .add("meterid", StringType)
        .add("location", StringType)
        .add("consumption", StringType)


      stream.foreachRDD { (rdd, time) =>
        val data = rdd.map(record => record.value)
        val json = spark.read.schema(schema).json(data)
        //json.createOrReplaceTempView("events")
        //spark.sql("select count(*) from events").show()
        json.coalesce(1).write.mode(SaveMode.Append).parquet(params.hdfsPath);



      }
      println("Starting Streaming Service")

      ssc.start()
      ssc.awaitTermination()

    }
  }
}