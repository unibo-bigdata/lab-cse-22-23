package exercise4;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.URI;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import utils.AwsCredentials;
import utils.Utils;

public class Ex4AverageWordLengthWithCombiner {

	public static class Ex4Mapper extends Mapper<Object, Text, Text, MyValue> {

		private Text word = new Text(), firstLetter = new Text();
		private IntWritable wordLength = new IntWritable();
		private IntWritable one = new IntWritable(1);

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString());
			while (itr.hasMoreTokens()) {
				word.set(itr.nextToken());
				firstLetter.set(word.toString().substring(0, 1));
				wordLength.set(word.getLength());
				context.write(firstLetter, new MyValue(wordLength,one));
			}
		}
	}

	public static class Ex4Combiner extends Reducer<Text, MyValue, Text, MyValue> {

		public void reduce(Text key, Iterable<MyValue> values, Context context)
				throws IOException, InterruptedException {

			int sum = 0, count = 0;
			for (MyValue val : values) {
				count += val.getCount().get();
				sum += val.getSum().get();
			}
			context.write(key, new MyValue(new IntWritable(sum),new IntWritable(count)));
		}
	}

	public static class Ex4Reducer extends Reducer<Text, MyValue, Text, DoubleWritable> {

		public void reduce(Text key, Iterable<MyValue> values, Context context)
				throws IOException, InterruptedException {

			int sum = 0, count = 0;
			for (MyValue val : values) {
				count += val.getCount().get();
				sum += val.getSum().get();
			}
			context.write(key, new DoubleWritable(sum / count));
		}
	}

	@SuppressWarnings("rawtypes")
	public static class MyValue implements WritableComparable {

		private IntWritable sum;
		private IntWritable count;

		public MyValue(IntWritable sum, IntWritable count) {
			this.sum = sum;
			this.count = count;
		} 

		public IntWritable getSum() {
			return this.sum;
		}

		public IntWritable getCount() {
			return this.count;
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			this.sum.readFields(in);
			this.count.readFields(in);
		}

		@Override
		public void write(DataOutput out) throws IOException {
			this.sum.write(out);
			this.count.write(out);
		}

		@Override 
		public int compareTo(Object o) {
			MyValue other = (MyValue) o;
			return this.count.compareTo(other.getCount());
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Average word length by initial letter");
		job.setJarByClass(Ex4AverageWordLengthWithCombiner.class);
		if (args.length > 3) {
			if (Integer.parseInt(args[3]) >= 0) {
				job.setNumReduceTasks(Integer.parseInt(args[3]));
			}
		} else {
			job.setNumReduceTasks(1);
		}

		AwsCredentials cred = Utils.credentialsFromFile();
		Configuration fsConf = new Configuration();
		fsConf.set("fs.s3a.impl", S3AFileSystem.class.getName());
		fsConf.set("fs.s3n.awsAccessKeyId", cred.getAccessKey());
		fsConf.set("fs.s3n.awsSecretAccessKey", cred.getSecretAccessKey());

		FileSystem fs = FileSystem.get(new URI(args[0]),fsConf);
		Path inputPath = new Path(args[1]), outputPath = new Path(args[2]);

		if (fs.exists(outputPath)) {
			fs.delete(outputPath, true);
		}

		job.setMapperClass(Ex4Mapper.class);
		job.setCombinerClass(Ex4Combiner.class);
		job.setReducerClass(Ex4Reducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(MyValue.class);
		FileInputFormat.addInputPath(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}