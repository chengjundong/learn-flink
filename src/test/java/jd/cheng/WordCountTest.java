package jd.cheng;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.junit.Test;

/**
 * 
 * @author jucheng
 *
 */
public class WordCountTest {
	
	/**
	 * Read an article and count the word
	 * 
	 * @throws Exception
	 */
	@SuppressWarnings("serial")
	@Test
	public void test() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStreamSource<String> src = env.readTextFile("A.txt");
		
		src.flatMap(new FlatMapFunction<String, String>() {
			@Override
			public void flatMap(String value, Collector<String> out) throws Exception {
				// split input article to words and collect them one by one
				for(String str : value.split(" ")) {
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
}
