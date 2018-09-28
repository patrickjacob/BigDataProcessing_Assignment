package pjdk.cooccurence.pairs;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import pjdk.cooccurence.pairsshared.WordPair;

/**
 * @author dimz
 * @since 28/9/18.
 */
public class PairsCountPartitioner
        extends Partitioner<WordPair, LongWritable> {
    private static Logger logger = LogManager.getLogger(PairsCountPartitioner.class);

    @Override
    public int getPartition(WordPair wordPair, LongWritable longWritable, int numReduceTasks) {
        logger.setLevel(Level.DEBUG);

        if(numReduceTasks == 0)
        {
            logger.debug("No partitioning - only ONE reducer");
            return 0;
        }

        // todo: implement dynamic split here based on input

        return 0;
    }
}
