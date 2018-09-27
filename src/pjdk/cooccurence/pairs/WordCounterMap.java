package pjdk.cooccurence.pairs;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.archive.io.ArchiveReader;
import org.archive.io.ArchiveRecord;

import java.io.IOException;

public class WordCounterMap {
    private static final Logger logger = Logger.getLogger(WordCounterMap.class);

    protected static enum MAPPERCOUNTER {
        RECORDS_IN,
        EMPTY_PAGE_TEXT,
        EXCEPTIONS,
        NON_PLAIN_TEXT
    }

    protected static class WordCountMapper extends Mapper<Text, ArchiveReader, WordPair, LongWritable> {
        private String[] tokens;
        private WordPair outKey = new WordPair();
        private LongWritable outVal = new LongWritable(1);

        static {
            logger.setLevel(Level.DEBUG);
        }

        @Override
        public void map(Text key, ArchiveReader value, Context context) throws IOException {
            int neighbors = context.getConfiguration().getInt("neighbors", 2);
            logger.warn("running mapper in: " + this.getClass().getSimpleName());
            for (ArchiveRecord r : value) {
                try {
                    if (r.getHeader().getMimetype().equals("text/plain")) {
                        context.getCounter(MAPPERCOUNTER.RECORDS_IN).increment(1);
                        logger.debug(r.getHeader().getUrl() + " -- " + r.available());
                        // Convenience function that reads the full message into a raw byte array
                        byte[] rawData = IOUtils.toByteArray(r, r.available());
                        String content = new String(rawData);
                        // Grab each word from the document
                        tokens = content.split("[\\W\\r\\n\\s\\d_MV]+");
                        /*
                        Match a single character present in the list below [\W\r\n\s\d]
                        \W matches any non-word character (equal to [^a-zA-Z0-9_])
                        \r matches a carriage return (ASCII 13)
                        \n matches a line-feed (newline) character (ASCII 10)
                        \s matches any whitespace character (equal to [\r\n\t\f\v ])
                        \d matches a digit (equal to [0-9])
                         */
                        if (tokens.length == 0) {
                            context.getCounter(MAPPERCOUNTER.EMPTY_PAGE_TEXT).increment(1);
                        } else {
                            for (int i = 0; i < tokens.length; i++) {
                                // skip letters
                                if (tokens[i].length() < 2) continue;
                                outKey.setWord(tokens[i]);
                                int start = (i - neighbors < 0) ? 0 : i - neighbors;
                                int end = (i + neighbors >= tokens.length) ? tokens.length - 1 : i + neighbors;
                                for (int j = start; j <= end; j++) {
                                    if (j == i || tokens[j].length() < 2) continue;
                                    outKey.setNeighbor(tokens[j]);
                                    context.write(outKey, outVal);
                                }
                            }
                        }
                    } else {
                        context.getCounter(MAPPERCOUNTER.NON_PLAIN_TEXT).increment(1);
                    }
                } catch (Exception ex) {
                    logger.error("Caught Exception", ex);
                    context.getCounter(MAPPERCOUNTER.EXCEPTIONS).increment(1);
                }
            }
        }
    }
}
