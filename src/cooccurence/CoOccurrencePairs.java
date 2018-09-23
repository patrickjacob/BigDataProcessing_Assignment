package cooccurence;

import org.apache.log4j.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class CoOccurrencePairs extends Configured implements Tool {
    private static final Logger LOG = Logger.getLogger(CoOccurrencePairs.class);

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new CoOccurrencePairs(), args));
    }

    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf = getConf();
        Job job = new Job(conf, "co-occurrence");
        job.setJarByClass(CoOccurrencePairs.class);

        // configure inputs
        String inputPath = "/tmp/*.warc.wet.gz";
        LOG.info("Input path: " + inputPath);
        FileInputFormat.addInputPath(job, new Path(inputPath));
        job.setInputFormatClass(WARCFileInputFormat.class);

        // add mapper
        job.setMapperClass(CoOccurrencePairsMapper.class);

        // setup output
        String outputPath = String.format("/tmp/%d/", System.currentTimeMillis());

        FileSystem fs = FileSystem.newInstance(conf);
        if (fs.exists(new Path(outputPath))) {
            fs.delete(new Path(outputPath), true);
        }
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        job.setOutputFormatClass(TextOutputFormat.class);

        job.setOutputKeyClass(WordPair.class);
        job.setOutputValueClass(LongWritable.class);

        job.setReducerClass(CoOccurencePairsReducer.class);
        job.setCombinerClass(CoOccurencePairsReducer.class);
        job.setNumReduceTasks(1);

        if (job.waitForCompletion(true)) {
            return 0;
        } else {
            return 1;
        }
    }
}
