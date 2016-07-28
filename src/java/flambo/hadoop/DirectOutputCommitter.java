package flambo.hadoop;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapred.*;

public class DirectOutputCommitter extends OutputCommitter {

  public static final Log LOG = LogFactory.getLog(DirectOutputCommitter.class);

  @Override
  public void setupJob(JobContext jobContext) {
    LOG.warn("setupJob or nah");
  }

  @Override
  public void setupTask(TaskAttemptContext taskContext) {
    LOG.warn("setupTask or nah");
  }

  @Override
  public boolean needsTaskCommit(TaskAttemptContext taskContext) {
    return true;
  }

  @Override
  public void commitTask(TaskAttemptContext taskContext) {
    LOG.warn("commitTask or nah");
  }

  @Override
  public void abortTask(TaskAttemptContext taskContext) {
    LOG.warn("abortTask or nah");
  }

  @Override
  public void commitJob(JobContext jobContext) throws IOException {
    JobConf conf = jobContext.getJobConf();
    if (shouldCreateSuccessFile(conf)) {
      Path outputPath = FileOutputFormat.getOutputPath(conf);
      if (outputPath != null) {
        FileSystem fs = outputPath.getFileSystem(conf);
        Path path = new Path(outputPath, FileOutputCommitter.SUCCEEDED_FILE_NAME);
        fs.create(path);
        fs.close();
      }
    }
  }

  private boolean shouldCreateSuccessFile(JobConf conf) {
    return conf.getBoolean("mapreduce.fileoutputcommitter.marksuccessfuljobs", true);
  }
}
