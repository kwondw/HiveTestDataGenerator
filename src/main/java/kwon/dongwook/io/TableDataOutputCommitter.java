package kwon.dongwook.io;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;

import java.io.IOException;

public class TableDataOutputCommitter extends FileOutputCommitter {

    // Since this application does not produce file at final destination,
    // as well as the fact it writes multiple outpus, default commit mechanism doesn't work

    private static final Log LOG = LogFactory.getLog(TableDataOutputCommitter.class);

    public TableDataOutputCommitter(Path outputPath, TaskAttemptContext context) throws IOException {
        super(outputPath, context);
    }

    public TableDataOutputCommitter(Path outputPath,
                               JobContext context) throws IOException {
        super(outputPath, context);
    }

    @Override
    public boolean needsTaskCommit(TaskAttemptContext context) throws IOException {
        return false;
    }

    @Override
    public void setupJob(JobContext context) throws IOException {
        LOG.info("no need to set up");
    }

    @Override
    public void commitJob(JobContext context) throws IOException {
        LOG.warn("No need to commit");
    }


}
