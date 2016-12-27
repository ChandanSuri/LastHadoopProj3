package org.eduonix;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.math.BigDecimal;

/**
 * Created by ubu on 5/4/14.
 */
public class AWSCloudProcessor  extends Configured implements Tool {

    static boolean isAmazon = true;
    private static final String BASE_URL = "s3://emrlearn";
    private static Path dataInEMR = new Path(BASE_URL+"/input" ,"ComercialBanks.csv");
    private static Path dataOutEMR  = new Path( BASE_URL+"/output");
    private static Path hdfsInput;
    private static Path hdfsOutput;

    private static final String clouderaTestBaseLocal = "/home/cloudera/clusterInput";
    private static final String END_CLUSTER_FLAG = "END_CLUSTER_FLAG";
    private static final String START_CLUSTER_FLAG = "START_CLUSTER_FLAG";
    private static Path mappedDataPath;
    private static Configuration conf;

    public static void main(String[] args) throws Exception
    {
        // this main function will call run method defined above.
        int res = ToolRunner.run(new Configuration(), new AWSCloudProcessor(), args);
        System.out.println("res: "+res);
        if(res==0  && !isAmazon) {
            runMigrate();
        }

        System.exit(res);
    }



    public int run(String[] strings) throws Exception {
        conf = getConf();
        Job job = null;
        hdfsInput  = new Path( "/user/cloudera/testData/ComercialBanks.csv");
        hdfsOutput = new Path( "/user/cloudera/testData");

        if(isAmazon){
            Configuration conf2 = new Configuration();
            job = new Job(conf2, "clusterProcessor");
        }
        else {
            job = Job.getInstance(conf);

        }

        job.setJarByClass(AWSCloudProcessor.class);
        job.setMapperClass(EntityMapper.class);
        job.setReducerClass(EntityReducerClusterSeeds.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        // these values define the types for the MAGIC shuffle sort steps
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        System.out.println("isAmazon  " + isAmazon);
        System.out.println("strings[0]  "+strings[0]);
        System.out.println("strings[1]  "+strings[1]);
        if(isAmazon){
           // FileInputFormat.addInputPath(job,  new Path("s3n://emrlearn/input/ComercialBanks.csv"));
           // FileOutputFormat.setOutputPath(job, new Path("s3n://emrlearn/ouput"));
           // org.eduonix.AWSCloudProcessor2  s3n://emrlearn/input/ComercialBanks.csv  s3n://emrlearn/ouput
            FileInputFormat.addInputPath(job, new Path(strings[0]));
            FileOutputFormat.setOutputPath(job, new Path(strings[1]));



        } else {
         //   FileSystem.get(hdfsInput.toUri(), conf).delete(hdfsOutput, true);
            FileInputFormat.addInputPath(job, hdfsInput);
            FileOutputFormat.setOutputPath(job, hdfsOutput);
        }

        return (job.waitForCompletion(true) ? 0 : 1);
    }

    public static int runMigrate( ) throws Exception {

        FileSystem fs = FileSystem.get(conf);
        mappedDataPath = new Path(clouderaTestBaseLocal);

        System.out.println( String.format("  mappedDataPath %s", mappedDataPath));

        fs.copyToLocalFile(false, hdfsOutput, mappedDataPath);

        return 0;
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
