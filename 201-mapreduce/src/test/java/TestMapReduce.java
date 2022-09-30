import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.StringTokenizer;

public class TestMapReduce {

    public static class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        protected Text word = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            StringTokenizer tokenizer = new StringTokenizer(line);
            while (tokenizer.hasMoreTokens()) {
                word.set(tokenizer.nextToken());
                context.write(word, new IntWritable(1));
            }
        }
    }

    public static class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

    @Test
    public void mapperBreakesTheRecord() throws IOException {
        new MapDriver<LongWritable, Text, Text, IntWritable>()
                .withMapper(new WordCountMapper())
                .withInput(new LongWritable(0), new Text("msg1 msg2 msg1"))
                .withAllOutput(Arrays.asList(
                        new Pair<>(new Text("msg1"), new IntWritable(1)),
                        new Pair<>(new Text("msg2"), new IntWritable(1)),
                        new Pair<>(new Text("msg1"), new IntWritable(1))
                ))
                .runTest();
    }

    @Test
    public void testSumReducer() throws IOException {
        new ReduceDriver<Text, IntWritable, Text, IntWritable>()
                .withReducer(new WordCountReducer())
                .withInput(new Text("msg1"), Arrays.asList(new IntWritable(1), new IntWritable(1)))
                .withOutput(new Text("msg1"), new IntWritable(2))
                .runTest();
    }

    @Test
    public void testWordCount() throws IOException {
        new MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, IntWritable>()
                .withMapper(new WordCountMapper())
                .withReducer(new WordCountReducer())
                .withInput(new LongWritable(0), new Text("msg1 msg2 msg1"))
                .withAllOutput(Arrays.asList(
                        new Pair<>(new Text("msg1"), new IntWritable(2)),
                        new Pair<>(new Text("msg2"), new IntWritable(1))
                ))
                .runTest();
    }
}
