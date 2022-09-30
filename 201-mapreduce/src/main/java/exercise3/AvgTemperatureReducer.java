package exercise3;
import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class AvgTemperatureReducer extends MapReduceBase implements Reducer<Text, IntWritable, Text, DoubleWritable> {

	public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, DoubleWritable> output,
			Reporter reporter) throws IOException {
		double tot = 0, count = 0;
		while (values.hasNext()) {
			count++;
			tot += (double) values.next().get() / 10;
		}
		output.collect(key, new DoubleWritable(tot / count));
	}
}
