package org.eduonix;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

/**
 * Created by ubu on 9/26/2015.
 *
 *  // org.eduonix.AWSCloudProcessor2  s3n://emrlearning/input  s3n://emrlearning/ouput

 */
public class AWSCloudProcessor2 {


    private static final String END_CLUSTER_FLAG = "END_CLUSTER_FLAG";
    private static final String START_CLUSTER_FLAG = "START_CLUSTER_FLAG";



    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration conf = new Configuration();
        Job job = new Job(conf, "clusterProcessor");

        job.setJarByClass(AWSCloudProcessor.class);
        job.setMapperClass(EntityMapper.class);
        job.setReducerClass(EntityReducerClusterSeeds.class);

        // these values define the types for the MAGIC shuffle sort steps
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        System.out.println("strings[0]  " + args[0]);
        System.out.println("strings[1]  " + args[1]);
        System.out.println("strings[2]  " + args[1]);
        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        job.submit();

        System.out.println(job);
        job.waitForCompletion(true);
    }







    public static class EntityMapper  extends Mapper<Object, Text, Text, Text>
    {
        private final static Text valueLine = new Text();
        private Text keyWord = new Text();
        public void map(Object key, Text value, Context context ) throws IOException, InterruptedException {

            String[] line = value.toString().split("\\t") ;
            if(line[0].equals("ID")) {
                return;
            }

            // set the name of the bank as our key
            keyWord.set(line[1]);
            valueLine.set(value);
            // emitt
            context.write(keyWord, valueLine);
        }
    }

    public static class EntityReducer  extends Reducer<Text,Text,Text,IntWritable> {

        public void reduce(Text key, Iterable<Text> values,  Context context) throws IOException, InterruptedException {

            int total = 0;

            for (Text val : values) {
                total++ ;
            }
            if( total > 1)  {
                // found a possible duplicate
                StringBuilder logger = new StringBuilder();
                logger.append("Found Duplicate Values for ").append(key).append("\n");

                for (Text val : values) {
                    logger.append(val).append("\n");
                }

                context.write( new Text(logger.toString()), new IntWritable(total));

            } else {
                context.write(key, new IntWritable(total));
            }


        }
    }


    public static class EntityReducerClusterSeeds  extends Reducer<Text,Text,Text,Text> {

        public void reduce(Text key, Iterable<Text> values,  Context context) throws IOException, InterruptedException {

            int total = 0;
            StringBuilder logger = new StringBuilder();
            logger.append(START_CLUSTER_FLAG).append("\n");
            for (Text val : values) {
                total++ ;
                logger.append(key).append("\t").append(val).append("\n");
            }
            if( total > 1)  {

                context.write(new Text(logger.toString()), new Text(END_CLUSTER_FLAG));

            }


        }
    }



}
