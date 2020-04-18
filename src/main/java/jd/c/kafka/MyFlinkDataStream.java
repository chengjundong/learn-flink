package jd.c.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;
import java.util.Properties;
import jd.c.datatype.PayWithBalanceTrx;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

public class MyFlinkDataStream {

  private static final ObjectMapper JACKSON = new ObjectMapper();

  public static void main(String[] args) throws Exception {
    Properties props = new Properties();
    props.setProperty("bootstrap.servers", "192.168.99.100:32768");

    StreamExecutionEnvironment env = StreamExecutionEnvironment
        .getExecutionEnvironment();

    DataStreamSource<String> src = env
        .addSource(new FlinkKafkaConsumer<>("jared", new SimpleStringSchema(), props));

    // 1. convert all input trx to object
    // 2. map to a tuple of $.trxType and count 1
    // 3. key by $.trxType
    // 4. get SUM of count
    // 5. print
    src.process(new ProcessFunction<String, PayWithBalanceTrx>() {
      @Override
      public void processElement(String s, Context context, Collector<PayWithBalanceTrx> collector)
          throws Exception {
        PayWithBalanceTrx trx = JACKSON.readValue(s, PayWithBalanceTrx.class);
        collector.collect(trx);
      }
    }).map(new MapFunction<PayWithBalanceTrx, Tuple2<String, Integer>>() {

      @Override
      public Tuple2<String, Integer> map(PayWithBalanceTrx trx) throws Exception {
        return Tuple2.of(trx.getTrxType(), 1);
      }
    }).keyBy(0).sum(1).print();

    env.execute();
  }
}
