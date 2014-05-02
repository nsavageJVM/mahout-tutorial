package org.eduonix.datagenerator;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is a mapreduce implementation of a generator of a large sentiment
 * analysis data set. The scenario is as follows:
 * 
 * The number of records will (roughly) correspond to the output size - each record is about 80 bytes.
 * 
 * 1KB set store_records=10
 * 1MB set store_records=10,000
 * 1GB set store_records=10,000,000
 * 1TB set store_records=10,000,000,000
 */
public class StoreJob {

    final static Logger log=LoggerFactory.getLogger(StoreJob.class);

    public enum props {

       store_records
    }

    public static Job createJob(Path output,Configuration conf) throws IOException{


        Job job=new Job(conf, "StoreTransaction_"+System.currentTimeMillis());
        // recursively delete the data set if it exists.
        FileSystem.get(conf).delete(output, true);
        job.setJarByClass(StoreJob.class);
        job.setMapperClass(MyMapper.class);
        // use the default reducer
        // job.setReducerClass(PetStoreTransactionGeneratorJob.Red.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setInputFormatClass(GenerateStoreTransactionsInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job, output);
        return job;
    }

    public static class MyMapper extends Mapper<Text, Text, Text, Text>{

        @Override
        protected void setup(Context context) throws IOException, InterruptedException{
            super.setup(context);
        }

        protected void map(Text key,Text value,Context context) throws java.io.IOException, InterruptedException{
            context.write(key, value);
            //TODO: Add multiple outputs here which writes mock addresses for generated users
            //to a corresponding data file.
        };
    }

    public static void main(String args[]) throws Exception{
            if(args.length!=2){
                System.err.println("USAGE : [number of records] [output path]");
                System.exit(0);
            }
            else{
                Configuration conf=new Configuration();
                conf.setInt("totalRecords", Integer.parseInt(args[0]));
                createJob(new Path(args[1]), conf).waitForCompletion(true);
            }
        }

}