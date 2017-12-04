package edu.fit.cse5310;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

public class Sentiments {
    // Taken from https://github.com/cleuton/bigdatasample
    public static class SentimentMapper
            extends Mapper<Object, Text, Text, IntWritable>{

        private final IntWritable one = new IntWritable(1);
        private Text word = new Text();
        private List<String> linhas;
        private SentiWordNetDemoCode sdc;

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // timestamp, screenName, tweetText, retweetCount, favoriteCount, hashtags, isOriginalContent
            if (this.linhas == null) {
                getSentiFile(context);
            }
            String tweet = MiscUtils.fieldsFromLine(key.toString())[2];
            String senti = sdc.analyze(tweet).toString();
            word.set(senti);
            context.write(word, one);
        }

        private void getSentiFile(Context context) throws IOException {
            Configuration conf = context.getConfiguration();
            String swnPath = conf.get("sentwordnetfile");
            System.out.println("@@@ Path: " + swnPath);
            this.linhas = new ArrayList<String>();
            try{
                Path pt=new Path(swnPath);
                FileSystem fs = FileSystem.get(new Configuration());
                BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(pt)));
                String line;
                line=br.readLine();
                while (line != null){
                    linhas.add(line);
                    line=br.readLine();
                }
            }catch(Exception e){
                System.out.println("@@@@ ERRO: " + e.getMessage());
                throw new IOException(e);
            }
            sdc = new SentiWordNetDemoCode(linhas);
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
