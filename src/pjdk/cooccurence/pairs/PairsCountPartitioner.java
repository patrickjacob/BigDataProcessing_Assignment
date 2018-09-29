package pjdk.cooccurence.pairs;

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

    private final static String alphabet = "abcdefghijklmnopqrstuvwxyz";

    /**
     * dynamically splits the data between partitioner based on the number of reduce tasks
     */

    @Override
    public int getPartition(WordPair wordPair, LongWritable longWritable, int numReduceTasks) {
        logger.setLevel(Level.DEBUG);

        if(numReduceTasks == 0)
        {
            logger.debug("No partitioning - only ONE reducer");
            return 0;
        }
        char firsLetter = wordPair.getWord().toString().toLowerCase().charAt(0);
        int letterInAlphabet = alphabet.indexOf(firsLetter);

        if(letterInAlphabet < 8){
            return 0;
        } else if (letterInAlphabet >= 9 && letterInAlphabet <=17) {
            return 1 % numReduceTasks;
        } else {
            return 2 % numReduceTasks;
        }




    }
}
