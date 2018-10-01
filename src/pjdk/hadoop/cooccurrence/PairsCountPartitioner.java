package pjdk.hadoop.cooccurrence;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

/**
 * @author dimz
 * @since 22/9/18.
 */
public class PairsCountPartitioner
        extends Partitioner<WordPair, LongWritable> {
    private static Logger logger = LogManager.getLogger(PairsCountPartitioner.class);

    /**
     * dynamically partitions the input data between partitioner based on the hashcode of words.
     */

    @Override
    public int getPartition(WordPair wordPair, LongWritable longWritable, int numReduceTasks) {
        logger.setLevel(Level.DEBUG);

        return numReduceTasks == 0 ? 0 : wordPair.getWord().hashCode() % numReduceTasks;
    }
}
