package exercise2;
import java.io.IOException;
import java.net.URI;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import utils.AwsCredentials;
import utils.Utils;

public class WordLengthCount {

	public static class TokenizerMapper
	extends Mapper<Object, Text, IntWritable, IntWritable>{

		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();
		private IntWritable wordLength = new IntWritable();

		public void map(Object key, Text value, Context context
				) throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString());
			while (itr.hasMoreTokens()) {
				word.set(itr.nextToken());
				wordLength.set(word.getLength());
				context.write(wordLength, one);
			}
		}
	}

	public static class IntSumReducer
	extends Reducer<IntWritable,IntWritable,IntWritable,IntWritable> {
		private IntWritable result = new IntWritable();

		public void reduce(IntWritable key, Iterable<IntWritable> values,
				Context context
				) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Word Length Count");

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

		job.setJarByClass(WordLengthCount.class);
		job.setMapperClass(TokenizerMapper.class);

		job.setCombinerClass(IntSumReducer.class);
		if(args.length>3){
			if(Integer.parseInt(args[3])>=0){
				job.setNumReduceTasks(Integer.parseInt(args[3]));
			}
		}
		else{
			job.setNumReduceTasks(1);
		}

		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);

		FileInputFormat.addInputPath(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}