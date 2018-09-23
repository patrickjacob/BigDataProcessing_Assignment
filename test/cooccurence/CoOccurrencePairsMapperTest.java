package cooccurence;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;

import org.junit.Test;

public class CoOccurrencePairsMapperTest {
    private IntWritable one = new IntWritable(1);
    private WordPair wp(String word, String neighbor) {
        return new WordPair(word,neighbor);
    }

    @Test
    public void testOneTokenInput() throws Exception {
        new MapDriver<LongWritable,Text,WordPair,IntWritable>()
                .withMapper(new PairsOccurrenceMapper())
                .withInput(new LongWritable(1l),new Text("Foo    "))
                .runTest();
    }
}