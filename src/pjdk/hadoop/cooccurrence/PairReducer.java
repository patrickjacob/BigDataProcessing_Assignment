package pjdk.hadoop.cooccurrence;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.reduce.LongSumReducer;

import java.io.IOException;


/**
 * @author dimz, patrick
 * @since 21/9/18.
 */
public class PairReducer extends LongSumReducer<WordPair> {
    // could have gotten away with just a simple LongSumReducer but need to implement one manually
    private LongWritable result = new LongWritable();

    @Override
    public void reduce(WordPair wordPair,
                       Iterable<LongWritable> values,
                       Reducer<WordPair, LongWritable, WordPair, LongWritable>.Context context)
            throws IOException, InterruptedException {
        long count = 0L;


        for(LongWritable value : values) {
            count += value.get();
        }

        result.set(count);
        context.write(wordPair, this.result);
    }
}
