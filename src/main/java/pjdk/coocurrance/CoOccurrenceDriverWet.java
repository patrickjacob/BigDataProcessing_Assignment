package pjdk.coocurrance;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

/**
 * User: Bill Bejeck
 * Date: 9/10/12
 * Time: 9:42 PM
 */
public class CoOccurrenceDriverWet extends Configured implements Tool {
    private static final Logger log = Logger.getLogger(CoOccurrenceDriverWet.class);

    @SuppressWarnings("Duplicates")
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new CoOccurrenceDriverWet(), args);
        System.exit(res);
    }


    @SuppressWarnings("Duplicates")
    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        Job job = Job.getInstance(conf);
        job.setJarByClass(CoOccurrenceDriverWet.class);
        job.setNumReduceTasks(1);

        job.setOutputFormatClass(TextOutputFormat.class);

        if (args[0].equalsIgnoreCase("pairs")) {
            job.setOutputKeyClass(WordPair.class);
            job.setOutputValueClass(IntWritable.class);
            doMapReduce(job, args[1], PairsOccurrenceMapper.class, "pairs", "pairs-co-occur",
                    PairsReducer.class, conf);
        } else if (args[0].equalsIgnoreCase("stripes")) {
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(MapWritable.class);
            doMapReduce(job, args[1], StripesOccurrenceMapper.class, "stripes", "stripes co-occur",
                    StripesReducer.class, conf);
        }

        return 0;
    }

    @SuppressWarnings("Duplicates")
    private static void doMapReduce(Job job,
                                    String path,
                                    Class<? extends Mapper> mapperClass,
                                    String outPath, String jobName,
                                    Class<? extends Reducer> reducerClass,
                                    Configuration conf) throws Exception {
        try {

            // output path setup
            String outputPath = String.format("/tmp/co/%s", outPath);
            FileSystem fs = FileSystem.newInstance(conf);
            if (fs.exists(new Path(outputPath))) {
                fs.delete(new Path(outputPath), true);
            }


            job.setJobName(jobName);
            FileInputFormat.addInputPath(job, new Path(path));
            FileOutputFormat.setOutputPath(job, new Path(outputPath));
            job.setMapperClass(mapperClass);
            job.setReducerClass(reducerClass);
            job.setCombinerClass(reducerClass);
            System.exit(job.waitForCompletion(true) ? 0 : 1);
        } catch (Exception e) {
            log.error("Error running MapReduce Job", e);
        }
    }

}
