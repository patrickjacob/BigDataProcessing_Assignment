package pjdk.hadoop.tsv;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import pjdk.hadoop.cooccurrence.WordPair;

/**
 * @author dimz
 * @since 1/10/18.
 */
public class CoOccurrenceTsv {
    private static Logger logger = LogManager.getLogger(CoOccurrenceTsv.class);
    public static void main(String[] args) throws Exception {
        Job job = Job.getInstance(new Configuration());
        job.setJarByClass(CoOccurrenceTsv.class);
        if (args[0].equalsIgnoreCase("pairs")) {
            job.setOutputKeyClass(WordPair.class);
            job.setOutputValueClass(IntWritable.class);
            doMapReduce(job, args[1], PairsOccurrenceMapper.class, "pairs", "pairs-co-occur", PairsReducer.class);
        } else if (args[0].equalsIgnoreCase("stripes")) {
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(MapWritable.class);
            doMapReduce(job, args[1], StripesOccurrenceMapper.class, "stripes", "stripes co-occur", StripesReducer.class);
        }

    }

    private static void doMapReduce(Job job,
                                    String path, Class<? extends Mapper> mapperClass, String outPath, String jobName, Class<? extends Reducer> reducerClass) throws Exception {
        try {
            job.setJobName(jobName);
            FileInputFormat.addInputPath(job, new Path(path));
            FileOutputFormat.setOutputPath(job, new Path(outPath));
            job.setMapperClass(mapperClass);
            job.setReducerClass(reducerClass);
            job.setCombinerClass(reducerClass);
            System.exit(job.waitForCompletion(true) ? 0 : 1);
        } catch (Exception e) {
            logger.error("Error running MapReduce Job", e);
        }
    }


}
