import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import scopt.OptionParser



object KafkaToHBase {

  case class Params(kafkaBrokers : String,
                    kafkaGroupID : String,
                    kafkaTopics : String,
                    appName : String,
                    duration : String)


  def main(args: Array[String]): Unit = {

    val parser = new OptionParser[Params]("scopt") {
      opt[String]('b', "kafkabrokers").action((x, c) => c.copy(kafkaBrokers = x)).text("Comma Seperated Kafka Brokers")
      opt[String]('g', "kafkagroupid").action((x, c) => c.copy(kafkaGroupID = x)).text("Kafka Group ID")
      opt[String]('t', "kafkatopics").action((x, c) => c.copy(kafkaTopics = x)).text("Comma Seperated Kafka Topics")
      opt[String]('n', "appname").action((x, c) => c.copy(appName = x)).text("Comma Seperated Kafka Topics")
      opt[String]('d', "duration").action((x, c) => c.copy(duration = x)).text("Streaming Batch Duration")


    }

    parser.parse(args, Params("", "", "", "", "")) match {
      case Some(config) => {
        // do stuff
        println("App Name : " + config.appName)
        println("Kafka Broker :" + config.kafkaBrokers)
        println("Kafka Group ID : " + config.kafkaGroupID)
        println("Kafka Topics : " + config.kafkaTopics)
        println("Batch Duration  : " + config.duration)

        run(config)
      }
      case None =>

        println("Bad Arguments")
      // arguments are bad, error message will have been displayed
    }

    def run(params: Params){

      val spark = SparkSession
        .builder()
        .appName(params.appName)
        .getOrCreate()


      val ssc = new StreamingContext(spark.sparkContext, Seconds(params.duration.toInt))


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

      stream.map(x => x.value()).foreachRDD { rdd =>
        rdd.foreachPartition { iter =>

          val conf: Configuration = HBaseConfiguration.create()
          val connection = ConnectionFactory.createConnection(conf)

          val ami_t = connection.getTable(TableName.valueOf(Bytes.toBytes("ami")))
          val yearly_t = connection.getTable(TableName.valueOf(Bytes.toBytes("amiyearly")))
          val monthly_t = connection.getTable(TableName.valueOf(Bytes.toBytes("amimonthly")))
          val daily_t = connection.getTable(TableName.valueOf(Bytes.toBytes("amidaily")))


          iter.foreach { a =>


            val readingJson = scala.util.parsing.json.JSON.parseFull(a)

            val readingMap: Map[String, String] = readingJson.get.asInstanceOf[Map[String, String]]


            val date = readingMap.get("date").get
            val split = a.split(",")

            val year = date.substring(0, 4)
            val month = date.substring(4, 6)
            val day = date.substring(6, 8)
            val hour = date.substring(8, 12)

            val yearmonth = date.substring(0, 6)
            val yearmonthday = date.substring(0, 8)
            val yearmonthdayhour = date.substring(0, 12)
            val consumption = readingMap.get("consumption").get
            val meterid = readingMap.get("meterid").get

            ami_t.incrementColumnValue((meterid).getBytes, "cf1".getBytes, year.getBytes, consumption.toLong)
            yearly_t.incrementColumnValue((meterid + year).getBytes, "cf1".getBytes, month.getBytes, consumption.toLong)
            monthly_t.incrementColumnValue((meterid + yearmonth).getBytes, "cf1".getBytes, day.getBytes, consumption.toLong)
            daily_t.incrementColumnValue((meterid + yearmonthday).getBytes, "cf1".getBytes, hour.getBytes, consumption.toLong)
          }

          ami_t.close()
          yearly_t.close()
          monthly_t.close()
          daily_t.close()

          connection.close()
          //  connection.close()
        }
      }


      println("Starting Streaming Service")

      ssc.start()
      ssc.awaitTermination()

    }
  }

}
