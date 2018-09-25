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
import org.apache.log4j.FileAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;


/**
 *  // start measurement for vertexes addition to a graph
 * initData();
 * endTime = System.nanoTime();
 * System.out.println(" Init Time: " + ((double) (endTime - startTime)) / Math.pow(10, 9) + " sec");
 */
public class CoOccurrenceDriverWet extends Configured implements Tool {

    private static final Logger logger = Logger.getLogger(CoOccurrenceDriverWet.class);

    @SuppressWarnings("Duplicates")
    public static void main(String[] args) throws Exception {
        // programmatically overriding log properties as can't access properties file for hadoop runner
        FileAppender fa = new FileAppender();
        fa.setName("FileLogger");
        fa.setFile("mylog.log");
        fa.setLayout(new PatternLayout("%d %-5p [%c{1}] %m%n"));
        fa.setThreshold(Level.INFO);
        fa.setAppend(true);
        fa.activateOptions();
        logger.addAppender(fa);
        // output start to the console
        logger.info("running co-occurence counter (info): " + args[0]);
        long startTime = System.nanoTime();
        int res = ToolRunner.run(new Configuration(), new CoOccurrenceDriverWet(), args);
        double runtime = (System.nanoTime() - startTime / Math.pow(10, 9));
        logger.info("Job Running Time: " + runtime);

        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        Job job = Job.getInstance(conf);
        job.setJarByClass(CoOccurrenceDriverWet.class);
        job.setNumReduceTasks(3); // 3 is the most optimal set up with original setup

        job.setOutputFormatClass(TextOutputFormat.class);

        int returnCode = 1;

        if (args[0].equalsIgnoreCase("pairs")) {
            job.setOutputKeyClass(WordPair.class);
            job.setOutputValueClass(IntWritable.class);
            returnCode = doMapReduce(job, args[1], PairsOccurrenceMapperWet.class, "pairs", "pairs-co-occur",
                    PairsReducer.class, conf);
        } else if (args[0].equalsIgnoreCase("stripes")) {
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(MapWritable.class);
            returnCode = doMapReduce(job, args[1], StripesOccurrenceMapper.class, "stripes", "stripes- co-occur",
                    StripesReducer.class, conf);
        }

        return returnCode;
    }


    private static int doMapReduce(Job job,
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
            return job.waitForCompletion(true) ? 0 : 1;
        } catch (Exception e) {
            logger.error("Error running MapReduce Job", e);
            return 1;
        }
    }
}
