package pjdk.hadoop.tsv;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.FileAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import pjdk.hadoop.cooccurrence.PairReducer;
import pjdk.hadoop.cooccurrence.PairsCountPartitioner;
import pjdk.hadoop.cooccurrence.StripesCoOccurrenceReducer;
import pjdk.hadoop.cooccurrence.WordPair;

import java.security.InvalidParameterException;
import java.util.concurrent.TimeUnit;

/**
 * @author dimz, patrick
 * @since 22/9/18.
 */
@SuppressWarnings("Duplicates")
public class TSVCoOccurrence extends Configured implements Tool {

    private static final Logger logger = Logger.getLogger(TSVCoOccurrence.class);

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
        int res = ToolRunner.run(new Configuration(), new TSVCoOccurrence(), args);
        runtime = System.nanoTime() - runtime;
        runtime = TimeUnit.SECONDS.convert(runtime, TimeUnit.NANOSECONDS);
        logger.info(String.format("Job Running Time: %d:%d with %d reducers",
                runtime / 60,
                runtime % 60,
                Integer.parseInt(args.length > 0 ? args[1] : "1"))
        );
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        Job job = Job.getInstance(conf);
        job.setJarByClass(this.getClass());
        job.setNumReduceTasks(Integer.parseInt(args.length > 0 ? args[1] : "1"));

        // input path setup
        String inputPath = String.format("%s/*.tsv", args[0]);
        logger.info("InputPath for file: " + inputPath);
        logger.info("Input path: " + inputPath);
        job.setInputFormatClass(TextInputFormat.class); // its a plain
        TextInputFormat.addInputPath(job, new Path(inputPath));

        String runnerType = (args.length > 2  && !args[2].isEmpty() ? args[2] : "occurrence" ).toLowerCase();

        String outputPath = String.format("%s/%s/%d/",
                args[0],
                runnerType,
                System.nanoTime());
        logger.info("output path: " + outputPath);
        FileSystem fs = FileSystem.newInstance(conf);
        if (fs.exists(new Path(outputPath))) {
            fs.delete(new Path(outputPath), true);
        }
        job.setInputFormatClass(TextInputFormat.class); // its a plain
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, new Path(outputPath));

        if(runnerType.contains("occurrence")){
            job.setOutputKeyClass(WordPair.class);
            job.setOutputValueClass(LongWritable.class);
            job.setReducerClass(PairReducer.class);
            switch (runnerType){
                case "occurrence":
                    job.setJobName("TsvWordPair Co-occurrence");
                    job.setMapperClass(TSVOccurrenceMapper.CoOccurrenceMapper.class);
                    break;
                case "occurrencecombiner":
                    job.setJobName("TsvWordPair Co-occurrence With Combiner");
                    job.setMapperClass(TSVOccurrenceMapper.CoOccurrenceMapper.class);
                    // set the combiner class. Should be the same as reducer
                    job.setCombinerClass(PairReducer.class);
                    break;
                case "occurrencepartitioner":
                    job.setJobName("TsvWordPair Co-occurrence With Partitioner");
                    job.setMapperClass(TSVOccurrenceMapper.CoOccurrenceMapper.class);
                    // set the combiner class. Should be the same as reducer
                    job.setPartitionerClass(PairsCountPartitioner.class);
                    break;
                case "occurrenceinmaplocal":
                    job.setJobName("TsvWordPair Co-occurrence With in-map aggregation local collection");
                    job.setMapperClass(TSVOccurrenceMapperInMapperLocal.CoOccurrenceMapperInMapper.class);
                    // set the combiner class. Should be the same as reducer
                    job.setPartitionerClass(PairsCountPartitioner.class);
                    break;
                case "occurrenceinmapglobal":
                    job.setJobName("TsvWordPair Co-occurrence With in-map aggregation global collection");
                    job.setMapperClass(TSVOccurrenceMapperInMapperGlobal.CoOccurrenceMapperInMapper.class);
                    // set the combiner class. Should be the same as reducer
                    job.setPartitionerClass(PairsCountPartitioner.class);
                    break;
            }
        } else if(runnerType.contains("stripes"))  {
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(MapWritable.class);

            job.setMapperClass(TSVStripesOccurrenceMapper.StripesCoOccurrenceMapper.class);
            // The reducer is quite useful in the word frequency task
            job.setReducerClass(StripesCoOccurrenceReducer.class);
            switch(runnerType){
                case "stripes":
                    job.setJobName("TsvStripes Co-occurrence");
                    break;
                case "stripescombiner":
                    job.setJobName("TsvStripes Co-occurrence With Combiner");
                    job.setCombinerClass(StripesCoOccurrenceReducer.class);
                    break;
            }
        } else {
            throw new InvalidParameterException("Job type parameter not set or unknown");
        }

        return job.waitForCompletion(true) ? 0 : 1;
    }
}
