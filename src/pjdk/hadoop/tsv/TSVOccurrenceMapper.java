package pjdk.hadoop.tsv;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import pjdk.hadoop.cooccurrence.WordPair;

import java.io.IOException;

/**
 * mapper for gzip archived file, map all headers and constructs
 * co-occurrence of predefined window value of 2
 *
 * @author dimz, patrick
 * @since 22/9/18.
 * @version 1.0
 */
@SuppressWarnings("Duplicates")
public class TSVOccurrenceMapper {
    private static final Logger logger = Logger.getLogger(TSVOccurrenceMapper.class);

    private static final int WINDOW_SIZE = 2;

    // counters visible in job on hue
    protected enum MAPPER_COUNTER {
        RECORDS_IN,
        EMPTY_PAGE_TEXT,
        EXCEPTIONS,
        HEADERS_LINE
    }

    protected static class CoOccurrenceMapper extends Mapper<LongWritable, Text, WordPair, LongWritable> {
        private String[] tokens;
        private WordPair outKey = new WordPair();
        private LongWritable outVal = new LongWritable(1);

        //set logger statically
        static {
            logger.setLevel(Level.DEBUG);
        }

        /**
         * mapper function
         * @param key not used in method,  id of WARC file
         * @param value pointer to WARC file
         */
        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            int neighbours = context.getConfiguration().getInt("neighbours", WINDOW_SIZE);
            logger.debug("running mapper in: " + this.getClass().getSimpleName());

            // split input line into values
            String[] line = value.toString().split("\t", -3);
                try {
                    if (!line[0].equals("marketplace")) { // if a header line
                        context.getCounter(MAPPER_COUNTER.RECORDS_IN).increment(1);
                        // Grab each word from the comment
                         /*
                        Match a single character present in the list below [\W\r\n\s\d]
                        \W matches any non-word character (equal to [^a-zA-Z0-9_])
                        \r matches a carriage return (ASCII 13)
                        \n matches a line-feed (newline) character (ASCII 10)
                        \s matches any whitespace character (equal to [\r\n\t\f\v ])
                        \d matches a digit (equal to [0-9])
                         */
                        tokens = line[13].split("[\\W\\r\\n\\s\\d_]+");
                        if (tokens.length == 0) {
                            context.getCounter(MAPPER_COUNTER.EMPTY_PAGE_TEXT).increment(1);
                        } else {
                            for (int i = 0; i < tokens.length; i++) { // skip one letter words
                                if (tokens[i].length() < 2) continue;
                                outKey.setWord(tokens[i]);
                                int start = (i - neighbours < 0) ? 0 : i - neighbours;
                                int end = (i + neighbours >= tokens.length) ? tokens.length - 1 : i + neighbours;
                                for (int j = start; j <= end; j++) {
                                    if (j == i || tokens[j].length() < 2) continue;
                                    outKey.setNeighbor(tokens[j]);
                                    context.write(outKey, outVal);
                                }
                            }
                        }
                    } else {
                        context.getCounter(MAPPER_COUNTER.HEADERS_LINE).increment(1);
                    }
                } catch (Exception ex) {
                    logger.error("Caught Exception", ex);
                    context.getCounter(MAPPER_COUNTER.EXCEPTIONS).increment(1);
                }

        }
    }
}
