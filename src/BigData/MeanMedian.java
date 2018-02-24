package BigData;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.output   .TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MeanMedian {

    public static class MeanMap extends Mapper<LongWritable, Text, LongWritable, PairWritable>{
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
            long num = Long.parseLong(value.toString());
            LongWritable number = new LongWritable(num);
            long numSq = num*num;
            LongWritable sqr = new LongWritable(numSq);
            LongWritable one = new LongWritable(1L);
            PairWritable pair = new PairWritable();
            pair.set(num,numSq);
            context.write(one,pair);

        }
    }

    public static class MeanReduce extends Reducer<LongWritable,PairWritable,Text,Text> {
        private LongWritable result = new LongWritable();
        @Override
        public void reduce(LongWritable key, Iterable<PairWritable> pairs, Context context) throws IOException, InterruptedException {
            long sum=0;
            long sqr=0;
            long count=0;
            for (PairWritable SinglePair:pairs
                 ) {
                sum +=SinglePair.getNum();
                sqr +=SinglePair.getNumsq();
                count++;
            }
            System.out.println("Count "+count);
            System.out.println("Sqr "+sqr);
            System.out.println("sum "+sum);
            double mean = (double) sum /count;
            double a = (double) (sqr /count);
            System.out.println("A "+a);
            double b = (double) mean*mean;
            System.out.println("B "+b);
            double variance = a - b;
            System.out.println("Variance "+variance);
            String output = Double.toString(mean)+"\t"+ Double.toString(variance);
            Text result = new Text(output);
            Text nULL = new Text("");
            context.write(nULL,result);

        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();		// get all args
        if (otherArgs.length != 2) {
            System.err.println("Usage: CountYelpReview <in> <out>");
            System.exit(2);
        }


        Job job = new Job(conf, "MeanMedian");
        job.setJarByClass(MeanMedian.class);

        job.setMapperClass(MeanMap.class);
        job.setReducerClass(MeanReduce.class);

        // set output key type
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // set output value type
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(PairWritable.class);




        //set the HDFS path of the input data
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        // set the HDFS path for the output
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        //Wait till job completion
        job.waitForCompletion(true);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
