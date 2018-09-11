package au.rmit.bde;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class TotalPhoneCalls extends Configured implements Tool {


    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new TotalPhoneCalls(), args));
    }

    /**
     * As hadoop is an execution framework, it provides APIs for us to specify how we want it to process the jobs.
     * Below are some essential APIs that hadoop provides.(defining input/output, defining mapper/reducer etc.)
     *
     *
     * TextInputFormat:
     * Given several input files, we want to tell Hadoop how the key-value pairs should be generated from our input files.
     * This is where InputFormat comes in. There are predefined several splitting rules provided by Hadoop,
     * Most common ones are listed below:
     *
     * @see org.apache.hadoop.mapreduce.lib.input.TextInputFormat
     *
     *      This is the default input format and we are using it for the sample. It splits a text file into key-value pairs
     *      where the key is the byte offset of the begining of the line, and the value is the content of the line.
     *      For example, consider the file containing the following contents:
     *              ******************
     *              *
     *              * id,comm_type,comm_identifier,comm_date,comm_time,comm_timedate_string,comm_timestamp,cell_tower_location,cell_tower_lat,cell_tower_long,cell_tower_state,cell_cgi
     *              * 1,Phone,f1a6836c0b7a3415a19a90fdd6f0ae18484d6d1e,01/04/2014,09:40:15,2014-04-01 09:40:15,1396305615,REDFERN TE,-33.89293048,151.2022686,NULL,50501015388B9
     *              * 2,Phone,62157ccf2910019ffd915b11fa037243b75c1624,01/04/2014,09:42:51,2014-04-01 09:42:51,1396305771,REDFERN TE,-33.89293048,151.2022686,NULL,50501015388B9
     *              *
     *              ******************
     *      Based on rules of TextInputFormat, two key-value pairs will be generated:
     *
     *      - key: 0    value: id,comm_type,comm_identifier,comm_date,comm_time,comm_timedate_string,comm_timestamp,cell_tower_location,cell_tower_lat,cell_tower_long,cell_tower_state,cell_cgi
     *      - key: 162  value: 1,Phone,f1a6836c0b7a3415a19a90fdd6f0ae18484d6d1e,01/04/2014,09:40:15,2014-04-01 09:40:15,1396305615,REDFERN TE,-33.89293048,151.2022686,NULL,50501015388B9
     *      - key: 317  value: 2,Phone,62157ccf2910019ffd915b11fa037243b75c1624,01/04/2014,09:42:51,2014-04-01 09:42:51,1396305771,REDFERN TE,-33.89293048,151.2022686,NULL,50501015388B9
     *
     * @see org.apache.hadoop.mapred.KeyValueTextInputFormat
     *      This is similar to TestInputFormat. It also operates on line basis.
     *      The input file should contains '\t' per line.
     *      The left part of '\t' will be treated as the key, while the right part will be treated as the value.
     *      For example, consider the file containing the following contents:
     *               ******************
     *               *
     *               * 1{\t}id,comm_type,comm_identifier,comm_date,comm_time,comm_timedate_string,comm_timestamp,cell_tower_location,cell_tower_lat,cell_tower_long,cell_tower_state,cell_cgi
     *               * 2{\t}1,Phone,f1a6836c0b7a3415a19a90fdd6f0ae18484d6d1e,01/04/2014,09:40:15,2014-04-01 09:40:15,1396305615,REDFERN TE,-33.89293048,151.2022686,NULL,50501015388B9
     *               * 3{\t}2,Phone,62157ccf2910019ffd915b11fa037243b75c1624,01/04/2014,09:42:51,2014-04-01 09:42:51,1396305771,REDFERN TE,-33.89293048,151.2022686,NULL,50501015388B9
     *               *
     *               ******************
     *      Based on rules of KeyValueTextInputFormat, two key-value pairs will be generated:
     *
     *      - key: 1    value: id,comm_type,comm_identifier,comm_date,comm_time,comm_timedate_string,comm_timestamp,cell_tower_location,cell_tower_lat,cell_tower_long,cell_tower_state,cell_cgi
     *      - key: 2    value: 1,Phone,f1a6836c0b7a3415a19a90fdd6f0ae18484d6d1e,01/04/2014,09:40:15,2014-04-01 09:40:15,1396305615,REDFERN TE,-33.89293048,151.2022686,NULL,50501015388B9
     *      - key: 3    value: 2,Phone,62157ccf2910019ffd915b11fa037243b75c1624,01/04/2014,09:42:51,2014-04-01 09:42:51,1396305771,REDFERN TE,-33.89293048,151.2022686,NULL,50501015388B9
     *
     * OutputFormat:
     * Like InputFormat, OutputFormat allows you to specify how the output of the mapReduce job would looks look.
     *
     * @see TextOutputFormat
     *      This is the default output format. It writes each key value pair per line separated by a separator.
     *
     *
     * ************* IMPORTANT *************
     *
     * In terms of implementation of mapReduce on Hadoop, one imporment thing you need to be aware of is that
     * combiner and reducer is the same thing more or less. There is no Combiner.class that you could implement
     * like you implement Mapper.class and Reducer.class.
     *
     * If you want to have a combiner, implement the Reducer.class. In this sample, we simply have one
     * algorithm {@code PhoneCountReducer#reduce()} as by both Combiner and Reducer. Checkout line 94 and line 96.
     * The difference of them is that combiner can only process output key-value pairs from mapper within the local machine,
     * while the reducer process them globally( i.e. after partitioning, the keys are grouped together and fed into reducer).
     *
     * Example:
     * Suppose we are runing our job on two worker node( say worker1 and worker2),
     * and suppose the combiner(reducer) on worker1 is triggered, while the one on worker 2 is not.
     *
     * Map phase:
     * Output by mapper on node1: (a:1), (b:1), (a:1)
     * Output by mapper on node2: (a:1), (b:1), (b:1)
     *
     * Combine phase:
     * Input to Combiner on node1: (a:(1,1)), (b:(1))
     * Output by Combiner on node1: (a:2), (b:1)
     * (since combiner on node2 is not triggered, the output is the on from Mapper phase)
     *
     * Reduce phase( suppose we have 1 reducer):
     *
     * Input: (a:(2, 1)), (b:(1, 1, 1))
     * Output: (a:3), (b:3)
     *
     * There are several things that deserve to mention,
     * - The input to the combiner is the same as the input to the reducer,
     *   and the output from mapper is the same to the one from combiner.
     * - In Combine phase, we can only process pairs within the local machine.
     * - The effectiveness is not affected no matter what combiner is executed or not.
     *
     */
    public int run(String[] args) throws Exception {
        Configuration configuration = getConf();

        configuration.set("mapreduce.job.jar", args[2]);
        //Initialising Map Reduce Job
        Job job = new Job(configuration, "Total Phone Calls");

        //Set Map Reduce main jobconf class
        job.setJarByClass(TotalPhoneCalls.class);

        //Set Mapper class
        job.setMapperClass(PhoneCountMapper.class);

        //Set Combiner class
        job.setCombinerClass(PhoneCountReducer.class);

        //set Reducer class
        job.setReducerClass(PhoneCountReducer.class);
        job.setNumReduceTasks(1);

        //set Input Format
        job.setInputFormatClass(TextInputFormat.class);

        //set Output Format
        job.setOutputFormatClass(TextOutputFormat.class);

        //set Output key class
        job.setOutputKeyClass(Text.class);

        //set Output value class
        job.setOutputValueClass(LongWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));


        return job.waitForCompletion(true) ? 0 : -1;
    }
}
