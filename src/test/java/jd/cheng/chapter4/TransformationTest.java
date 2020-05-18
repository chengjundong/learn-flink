package jd.cheng.chapter4;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.Test;

/**
 * @author jucheng
 */
public class TransformationTest {

  @Test
  public void testSingleMap() throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(1);
    // input is positive
    DataStreamSource<Integer> ds = env.fromElements(1, 2, 3, 4);

    // convert them to negative
    ds.map(new MapFunction<Integer, Integer>() {
      @Override
      public Integer map(Integer input) throws Exception {
        return -1 * input;
      }
    }).print();

    env.execute("testSingleMap");
  }

  @Test
  public void testReduce() throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(1);
    DataStreamSource<Tuple2<String, Integer>> ds1 = env
        .fromElements(Tuple2.of("a", 3), Tuple2.of("b", 2), Tuple2.of("a", 1), Tuple2.of("b", 5));

    DataStreamSource<Tuple2<String, Integer>> ds2 = env
        .fromElements(Tuple2.of("c", 1), Tuple2.of("b", 9), Tuple2.of("c", 4), Tuple2.of("c", 2));

    DataStream<Tuple2<String, Integer>> ds = ds1.union(ds2);

    ds.keyBy(0).reduce(new ReduceFunction<Tuple2<String, Integer>>() {
      @Override
      public Tuple2<String, Integer> reduce(Tuple2<String, Integer> var1,
          Tuple2<String, Integer> var2) throws Exception {
        return Tuple2.of(var2.f0, var2.f1 + var1.f1);
      }
    }).print();

    env.execute("testReduce");
  }
}
