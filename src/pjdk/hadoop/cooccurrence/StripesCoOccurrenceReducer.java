package pjdk.hadoop.cooccurrence;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author dimz
 * @since 30/9/18.
 */
public class StripesCoOccurrenceReducer extends Reducer<Text, MapWritable, Text, MapWritable> {
    private MapWritable valuesIncrementsMap = new MapWritable();

    @Override
    protected void reduce(Text key, Iterable<MapWritable> values, Context context) throws IOException, InterruptedException {
        valuesIncrementsMap.clear();
        for (MapWritable value : values) {
            addAll(value); // will it hold in memory, I wonder?
        }
        context.write(key, valuesIncrementsMap);
    }

    private void addAll(MapWritable mapWritable) {
        for (Writable key : mapWritable.keySet()) {
            IntWritable fromCount = (IntWritable) mapWritable.get(key);
            if (valuesIncrementsMap.containsKey(key)) {
                IntWritable count = (IntWritable) valuesIncrementsMap.get(key);
                count.set(count.get() + fromCount.get());
            } else {
                valuesIncrementsMap.put(key, fromCount);
            }
        }
    }

}
