package exercise3;
import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import utils.AwsCredentials;
import utils.Utils;

public class MaxTemperatureWithCombiner {
	public static void main(String[] args) throws Exception {
		JobConf conf = new JobConf(MaxTemperatureWithCombiner.class);
		conf.setJobName("Max temperature");

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

		FileInputFormat.addInputPath(conf, inputPath);
		FileOutputFormat.setOutputPath(conf, outputPath);

		conf.setMapperClass(MaxTemperatureMapper.class);
		conf.setCombinerClass(MaxTemperatureCombiner.class);
		conf.setReducerClass(MaxTemperatureReducer.class);

		if (args.length > 3 && Integer.parseInt(args[3]) >= 0) {
			conf.setNumReduceTasks(Integer.parseInt(args[3]));
		} else {
			conf.setNumReduceTasks(1);
		}

		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(IntWritable.class);
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(DoubleWritable.class);

		JobClient.runJob(conf);
	}
}
