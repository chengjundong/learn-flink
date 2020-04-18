package jd.c.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import jd.c.datatype.PayWithBalanceTrx;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

public class MyProducer {

  private static final ObjectMapper JACKSON = new ObjectMapper();

  public static void main(String[] args) throws Exception {
    Properties props = new Properties();
    props.put("bootstrap.servers", "192.168.99.100:32768");
    props.put("acks", "all");
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

    Producer<String, String> producer = new KafkaProducer<>(props);
    for (int i = 0; i < 10; i++) {
      PayWithBalanceTrx trx = new PayWithBalanceTrx();
      trx.setTrxId(i);
      trx.setTrxType(i%2 == 0? "RECOUP" : "DISPUTE-HOLD");

      producer.send(
          new ProducerRecord<>("jared", JACKSON.writeValueAsString(trx)));
    }

    producer.close();
  }
}
