package exercise4;
import java.io.IOException;
import java.net.URI;
import java.util.StringTokenizer;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import utils.AwsCredentials;
import utils.Utils;

public class Ex4InvertedIndex {

	public static class Ex4Mapper extends Mapper<Object, Text, Text, LongWritable> {

		private Text word = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString());
			while (itr.hasMoreTokens()) {
				word.set(itr.nextToken());
				context.write(word, (LongWritable) key);
				System.out.println(word + ": " + key);
			}
		}
	}

	public static class Ex4Reducer extends Reducer<Text, LongWritable, Text, Text> {

		public void reduce(Text key, Iterable<LongWritable> values, Context context)
				throws IOException, InterruptedException {

			TreeSet<Long> offsets = new TreeSet<Long>();

			for (LongWritable val : values) {
				offsets.add(val.get());
			}

			context.write(key, new Text(offsets.toString()));
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Average word length by initial letter");
		job.setJarByClass(Ex4InvertedIndex.class);
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
		job.setReducerClass(Ex4Reducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}