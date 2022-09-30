package exercise3;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class AvgTemperatureMapper extends MapReduceBase 
implements Mapper<LongWritable, Text, Text, IntWritable> {
	private static final int MISSING = 9999;
	public void map(LongWritable key, Text value, 
			OutputCollector<Text,IntWritable> output, Reporter reporter) 
					throws IOException {
		String line = value.toString();

		String yearAndMonth = line.substring(15, 21);
		int airTemperature = Integer.parseInt(line.substring(87, 92));

		if ( airTemperature != MISSING ) {
			output.collect(new Text(yearAndMonth), new IntWritable(airTemperature));
		}
	}
}
