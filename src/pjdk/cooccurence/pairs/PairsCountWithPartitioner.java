package pjdk.cooccurence.pairs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.LongSumReducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.FileAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import pjdk.cooccurence.pairsshared.WordPair;
import pjdk.cooccurence.warc.WARCFileInputFormat;

import java.util.concurrent.TimeUnit;


@SuppressWarnings("Duplicates")
public class PairsCountWithPartitioner extends Configured implements Tool {

    private static final Logger logger = Logger.getLogger(PairsCountWithPartitioner.class);

    static {
        FileAppender fa = new FileAppender();
        fa.setName("FileLogger");
        fa.setFile("mylog.log");
        fa.setLayout(new PatternLayout("%d %-5p [%c{1}] %m%n"));
        fa.setThreshold(Level.INFO);
        fa.setAppend(true);
        fa.activateOptions();
        logger.addAppender(fa);
    }

    public static void main(String[] args) throws Exception {
        long runtime = System.nanoTime();
        int res = ToolRunner.run(new Configuration(), new PairsCountWithPartitioner(), args);
        runtime = System.nanoTime() - runtime;
        runtime = TimeUnit.SECONDS.convert(runtime, TimeUnit.NANOSECONDS);
        logger.info(String.format("Job Running Time: %d:%d with %d reducers",
                runtime / 60,
                runtime % 60,
                Integer.parseInt(args[1].isEmpty() ? "1" : args[1] ))
        );
        System.exit(res);
    }

    @Override
    public int run(String[] arg0) throws Exception {
        Configuration conf = getConf();
        //
        Job job = Job.getInstance(conf);
        job.setJobName("WordPair Co-occurrence");
        job.setJarByClass(PairsCountWithPartitioner.class);
        job.setNumReduceTasks(Integer.parseInt(arg0[1].isEmpty() ? "1" : arg0[1] ));

        // input path setup
        String inputPath = String.format("%s/*.warc.wet.gz", arg0[0]);
        logger.info("InputPath for file: " + inputPath);
        logger.info("Input path: " + inputPath);
        FileInputFormat.addInputPath(job, new Path(inputPath));

        String outputPath = String.format("%s/%s/%d/",
                arg0[0],
                this.getClass().getSimpleName(),
                System.nanoTime());
        logger.info("output path: " + outputPath);
        FileSystem fs = FileSystem.newInstance(conf);
        if (fs.exists(new Path(outputPath))) {
            fs.delete(new Path(outputPath), true);
        }
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        job.setInputFormatClass(WARCFileInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setOutputKeyClass(WordPair.class);
        job.setOutputValueClass(LongWritable.class);

        job.setMapperClass(WordCounterMap.CoOccurrenceMapper.class);
        // set partitioner
        job.setPartitionerClass(PairsCountPartitioner.class);

        // The reducer is quite useful in the word frequency task
        job.setReducerClass(LongSumReducer.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }
}