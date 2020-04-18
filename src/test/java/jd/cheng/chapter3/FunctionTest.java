package jd.cheng.chapter3;

import jd.c.datatype.PayWithBalanceTrx;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;
import org.junit.Test;

/**
 * @author jucheng
 */
@SuppressWarnings("serial")
public class FunctionTest {

  /**
   * Read an article and count the word
   *
   * @throws Exception
   */
  @Test
  public void testWordCount() throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    DataStreamSource<String> src = env.readTextFile("A.txt");

    src.flatMap(new FlatMapFunction<String, String>() {
      @Override
      public void flatMap(String value, Collector<String> out) throws Exception {
        // split input article to words and collect them one by one
        for (String str : value.split(" ")) {
          out.collect(str);
        }
      }
    }).filter(new FilterFunction<String>() {
      @Override
      public boolean filter(String value) throws Exception {
        // filer the whitespace
        return StringUtils.isNoneBlank(value);
      }
    }).map(new MapFunction<String, Tuple2<String, Integer>>() {
      @Override
      public Tuple2<String, Integer> map(String value) throws Exception {
        return Tuple2.of(value, 1);
      }
    }).keyBy(0).sum(1).print();

    env.execute();
  }

  /**
   * To find all RECOUP transactions
   *
   * @throws Exception
   */
  @Test
  public void testKeyBy() throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(2);

    DataStreamSource<PayWithBalanceTrx> src = env
        .fromElements(new PayWithBalanceTrx(1, "RECOUP"), new PayWithBalanceTrx(2, "RECOUP"),
            new PayWithBalanceTrx(3, "DISPUTE_HOLD"),
            new PayWithBalanceTrx(4, "SHIPPING_LABEL_CHARGE"),
            new PayWithBalanceTrx(5, "RECOUP"), new PayWithBalanceTrx(6, "DISPUTE_HOLD"));

    src.keyBy("trxType") // use trxType as the partition key
        .countWindow(1) // once the partition size is >= 1
        .apply(
            new WindowFunction<PayWithBalanceTrx, PayWithBalanceTrx, Tuple, GlobalWindow>() { // collect all trx whose type is RECOUP
              public void apply(Tuple key, GlobalWindow window, Iterable<PayWithBalanceTrx> input,
                  Collector<PayWithBalanceTrx> out) throws Exception {
                String trxType = key.getField(0).toString();
                if ("RECOUP".equals(trxType)) {
                  input.forEach(trx -> out.collect(trx));
                }
              }

              ;
            }).print();

    env.execute();
  }
}
