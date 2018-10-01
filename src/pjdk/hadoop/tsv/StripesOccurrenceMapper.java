package pjdk.hadoop.tsv;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.archive.io.ArchiveReader;
import org.archive.io.ArchiveRecord;

import java.io.IOException;

/**
 * @author dimz, patrick
 * @since 30/9/18.
 */
@SuppressWarnings("Duplicates")
public class StripesOccurrenceMapper {
    private static Logger logger = LogManager.getLogger(StripesOccurrenceMapper.class);

    private static final int WINDOW_SIZE = 2;

    protected enum STRIPES_MAPPER_COUNTER {
        RECORDS_IN,
        EMPTY_PAGE_TEXT,
        EXCEPTIONS,
        NON_PLAIN_TEXT
    }

    protected static class StripesCoOccurrenceMapper extends Mapper<Text, ArchiveReader, Text, MapWritable> {
        private MapWritable occurrenceMap = new MapWritable();
        private Text word = new Text();

        static {
            logger.setLevel(Level.DEBUG);
        }

        /**
         * mapper function
         * @param key not used in method,  id of WARC file
         * @param value pointer to WARC file
         */
        @Override
        public void map(Text key, ArchiveReader value, Context context)
                throws IOException, InterruptedException {
            int neighbours = context.getConfiguration().getInt("neighbours", WINDOW_SIZE);
            logger.warn("running mapper in: " + this.getClass().getSimpleName());
            for (ArchiveRecord r : value) {
                try {
                    if (r.getHeader().getMimetype().equals("text/plain")) {
                        context.getCounter(STRIPES_MAPPER_COUNTER.RECORDS_IN).increment(1);
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
                        String[] tokens = content.split("[\\W\\r\\n\\s\\d_]+");
                        if (tokens.length == 0) {
                            context.getCounter(STRIPES_MAPPER_COUNTER.EMPTY_PAGE_TEXT).increment(1);
                        } else {
                            for (int i = 0; i < tokens.length; i++) { // skip one letter words
                                word.set(tokens[i]);
                                occurrenceMap.clear();

                                int start = (i - neighbours < 0) ? 0 : i - neighbours;
                                int end = (i + neighbours >= tokens.length) ? tokens.length - 1 : i + neighbours;
                                for (int j = start; j <= end; j++) {
                                    if (j == i) continue;
                                    Text neighbor = new Text(tokens[j]);
                                    if(occurrenceMap.containsKey(neighbor)){
                                        IntWritable count = (IntWritable)occurrenceMap.get(neighbor);
                                        count.set(count.get()+1);
                                    }else{
                                        occurrenceMap.put(neighbor,new IntWritable(1));
                                    }
                                }
                                context.write(word,occurrenceMap);
                            }
                        }
                    } else {
                        context.getCounter(STRIPES_MAPPER_COUNTER.NON_PLAIN_TEXT).increment(1);
                    }
                } catch (Exception ex) {
                    logger.error("Caught Exception", ex);
                    context.getCounter(STRIPES_MAPPER_COUNTER.EXCEPTIONS).increment(1);
                }
            }
        }
    }
}
