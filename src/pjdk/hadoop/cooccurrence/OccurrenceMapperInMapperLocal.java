package pjdk.hadoop.cooccurrence;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.archive.io.ArchiveReader;
import org.archive.io.ArchiveRecord;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * mapper for gzip archived file, map all headers and constructs
 * co-occurrence of predefined window value of 2
 *
 * @author dimz, patrick
 * @version 1.0
 * @since 22/9/18.
 */
@SuppressWarnings("Duplicates")
public class OccurrenceMapperInMapperLocal {
    private static final Logger logger = Logger.getLogger(OccurrenceMapperInMapperLocal.class);

    private static final int WINDOW_SIZE = 2;

    // counters visible in job on hue
    protected enum MAPPER_COUNTER {
        RECORDS_IN,
        EMPTY_PAGE_TEXT,
        EXCEPTIONS,
        NON_PLAIN_TEXT
    }

    protected static class CoOccurrenceMapperInMapper extends Mapper<Text, ArchiveReader, WordPair, LongWritable> {

        //set logger statically
        static {
            logger.setLevel(Level.DEBUG);
        }

        /**
         * mapper function
         *
         * @param key   not used in method,  id of WARC file
         * @param value pointer to WARC file
         */
        @Override
        public void map(Text key, ArchiveReader value, Context context)
                throws IOException, InterruptedException {

            int neighbours = context.getConfiguration().getInt("neighbours", WINDOW_SIZE);
            logger.debug("running mapper in: " + this.getClass().getSimpleName());

            String[] tokens;

            for (ArchiveRecord r : value) {
                Map<WordPair, Long> inMapperMap = new HashMap<>();
                try {
                    if (r.getHeader().getMimetype().equals("text/plain")) {
                        context.getCounter(MAPPER_COUNTER.RECORDS_IN).increment(1);
                        logger.debug(r.getHeader().getUrl() + " -- " + r.available());
                        // Convenience function that reads the full message into a raw byte array
                        byte[] rawData = IOUtils.toByteArray(r, r.available());
                        String content = new String(rawData);
                        // Grab each word from the document
                         /*
                        Match a single character present in the list below [\W\r\n\s\d]
                        \W matches any non-word character (equal to [^a-zA-Z0-9_])
                        \r matches a carriage return (ASCII 13)
                        \n matches a line-feed (newline) character (ASCII 10)
                        \s matches any whitespace character (equal to [\r\n\t\f\v ])
                        \d matches a digit (equal to [0-9])
                         */

                        tokens = content.split("[\\W\\r\\n\\s\\d_]+");
                        if (tokens.length == 0) {
                            context.getCounter(MAPPER_COUNTER.EMPTY_PAGE_TEXT).increment(1);
                        } else {
                            // implementing an in-map optimizer
                            for (int i = 0; i < tokens.length; i++) {
                                if (tokens[i].length() < 2) continue;  // skip one letter words
                                int start = (i - neighbours < 0) ? 0 : i - neighbours;
                                int end = (i + neighbours >= tokens.length) ? tokens.length - 1 : i + neighbours;
                                for (int j = start; j <= end; j++) {
                                    if (j == i || tokens[j].length() < 2) continue;
                                    WordPair wordPair = new WordPair(tokens[i], tokens[j]);
                                    if (inMapperMap.containsKey(wordPair)) {
                                        long total = inMapperMap.get(wordPair) + 1;
                                        inMapperMap.put(wordPair, total);
                                    } else {
                                        inMapperMap.put(wordPair, 1L);
                                    }
                                }
                            }

                        }
                    } else {
                        context.getCounter(MAPPER_COUNTER.NON_PLAIN_TEXT).increment(1);
                    }
                } catch (Exception ex) {
                    logger.error("Caught Exception", ex);
                    context.getCounter(MAPPER_COUNTER.EXCEPTIONS).increment(1);
                }
                // output map to context
                logger.debug("MapSize: " + inMapperMap.size());
                for (Map.Entry<WordPair, Long> inMapperMapEntry : inMapperMap.entrySet()) {
                    context.write(inMapperMapEntry.getKey(), new LongWritable(inMapperMapEntry.getValue()));
                }
            }
        }
    }
}
