package kwon.dongwook.io;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class TableDataOutputFormat extends TextOutputFormat {

    private static final Log LOG = LogFactory.getLog(TableDataOutputFormat.class);

    private TableDataOutputCommitter committer = null;


    public synchronized OutputCommitter getOutputCommitter(TaskAttemptContext context)
            throws IOException {
        if (committer == null) {
            Path output = getOutputPath(context);
            committer = new TableDataOutputCommitter(output, context);
        }
        return committer;
    }

}
