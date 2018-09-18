package main.bigdata;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

//Reducer
public class PhoneCountReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
    private LongWritable total = new LongWritable();

    /**
     * After mapping, the pairs are streamed to reducers, in which the reduce algorithm are executed.
     *
     * @param token The key of the output pair from Mapper.
     * @param counts The list of values of the same key of pairs from Mapper.
     * @param context The bridge streams the reducer output to the framework that will finally write it to HDFS.
     */
    @Override
    protected void reduce(Text token, Iterable<LongWritable> counts, Context context)
            throws IOException, InterruptedException {
        long n = 0;
        //Calculate sum of counts
        for (LongWritable count : counts)
            n += count.get();
        total.set(n);

        context.write(token, total);
    }
}