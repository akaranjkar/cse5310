package edu.fit.cse5310;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Sentiments {
    public static class SentimentMapper extends Mapper<Object, Text, Text, IntWritable> {
        private IntWritable one = new IntWritable(1);
        private Text sentiment = new Text();
        private static String swnFile = "SentiWordNet_3.0.0_20130122.txt";
        private static SentiWordNetDemoCode swn;

        static {
            try {
                swn = new SentiWordNetDemoCode(swnFile);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        public void map(Object key, Text line, Context context) throws IOException, InterruptedException {
            String[] fields = MiscUtils.fieldsFromLine(line.toString());
            sentiment.set(swn.analyze(fields[2]).toString());
            context.write(sentiment,one);
        }

    }
    public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable value: values) {
                sum +=value.get();
            }
            result.set(sum);
            context.write(key,result);
        }
    }
}
