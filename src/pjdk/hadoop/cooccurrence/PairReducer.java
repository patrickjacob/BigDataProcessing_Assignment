package pjdk.hadoop.cooccurrence;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.reduce.LongSumReducer;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;


/**
 * @author dimz, patrick
 * @since 21/9/18.
 */
public class PairReducer extends LongSumReducer<WordPair> {
    // could have gotten away with just a simple LongSumReducer but need to implement one manually
    private LongWritable result = new LongWritable();
    private static Logger logger = LogManager.getLogger(LongSumReducer.class);

    //set logger statically
    static {
        logger.setLevel(Level.DEBUG);
    }

    @Override
    public void reduce(WordPair wordPair,
                       Iterable<LongWritable> values,
                       Reducer<WordPair, LongWritable, WordPair, LongWritable>.Context context)
            throws IOException, InterruptedException {

        logger.debug("running reduce task");
        context.getCounter(OccurrenceMapper.MAPPER_COUNTER.RECORDS_OUT).increment(1);

        long count = 0L;

        for (LongWritable value : values) {
            count += value.get();
        }
        result.set(count);
        context.write(wordPair, this.result);
    }
}
