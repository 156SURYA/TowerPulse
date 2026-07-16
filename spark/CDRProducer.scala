import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import java.util.Properties
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import scala.util.Random

object CDRProducer {

  def main(args: Array[String]): Unit = {

    val props = new Properties()
    props.put("bootstrap.servers", "localhost:9092")
    props.put("key.serializer",   "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    val producer  = new KafkaProducer[String, String](props)
    val random    = new Random()
    val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")

    val towers    = List("T1", "T2", "T3", "T4", "T5")
    val callTypes = List("VOICE", "SMS", "DATA")

    println("CDRProducer started. Publishing to topic: telecom_cdr")
    println("Press Ctrl+C to stop.\n")

    var callId = 1

    try {
      while (true) {
        val towerId   = towers(random.nextInt(towers.length))
        val callType  = callTypes(random.nextInt(callTypes.length))
        val caller    = 9000000000L + random.nextInt(999999999)
        val receiver  = 9000000000L + random.nextInt(999999999)
        val duration  = 10 + random.nextInt(300)
        val callDate  = LocalDateTime.now().format(formatter)

        val cdr =
          s"""{
             |  "call_id":   "C${callId}",
             |  "caller":    ${caller},
             |  "receiver":  ${receiver},
             |  "call_type": "${callType}",
             |  "duration":  ${duration},
             |  "call_date": "${callDate}",
             |  "tower_id":  "${towerId}"
             |}""".stripMargin

        producer.send(new ProducerRecord[String, String]("telecom_cdr", s"C${callId}", cdr))
        println(s"[CDR] call_id=C${callId}  tower=${towerId}  type=${callType}")

        callId += 1
        Thread.sleep(300)
      }
    } finally {
      producer.close()
      println("CDRProducer stopped.")
    }
  }
}
