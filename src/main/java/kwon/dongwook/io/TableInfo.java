package kwon.dongwook.io;

import kwon.dongwook.model.Partition;
import kwon.dongwook.model.Partitions;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

public class TableInfo implements WritableComparable<TableInfo> {

    private static Log LOG = LogFactory.getLog(TableInfo.class);

    private Text partitionPath;
    private Text basePath;
    private Text creationDDL;
    private boolean partitioned;
    private IntWritable outputFileIndex;

    public TableInfo() {
        this.partitionPath = new Text();
        this.creationDDL = new Text();
        this.partitioned = false;
        this.basePath = new Text("");
        this.outputFileIndex = new IntWritable(1);
    }

    public TableInfo(String strBasePath) {
        this();
        if(strBasePath.lastIndexOf("/") == (strBasePath.length() -1)) {
          strBasePath = strBasePath.substring(0, strBasePath.length() - 1);
        }
        this.basePath.append(strBasePath.getBytes(), 0, strBasePath.length());
        this.partitionPath.append(strBasePath.getBytes(), 0, strBasePath.length());
    }

    public void setPartitionPathsBy(Partitions partitions) {
        partitioned = true;
        ArrayList<Partition> parts = partitions.getPartitions();
        Iterator<Partition> itr = parts.iterator();
        StringBuilder sb = new StringBuilder(this.basePath.toString());
        while(itr.hasNext()) {
            Partition part = itr.next();
            sb.append("/")
                .append(part.getKeyName())
                .append("=")
                .append(part.getValue());
        }
        this.partitionPath = new Text(sb.toString());
    }

    public boolean isPartitioned() {
      return partitioned;
    }

    @Override
    public int compareTo(TableInfo o) {
        return this.getOutputPath().compareTo(o.getOutputPath());
    }

    @Override
    public void write(DataOutput out) throws IOException {
        outputFileIndex.write(out);
        if(isPartitioned()) {
          partitionPath.write(out);
        } else {
          basePath.write(out);
        }
        creationDDL.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        outputFileIndex.readFields(in);
        if(isPartitioned()) {
          partitionPath.readFields(in);
        } else {
          basePath.readFields(in);
        }
        creationDDL.readFields(in);
    }

    public Text getOutputPath() {
        return isPartitioned() ? partitionPath: new Text(basePath.toString() + "/");
    }

    public Text getCreationDDL() { return this.creationDDL; }

    public void setCreationDDL(Text ddl) {
        this.creationDDL = ddl;
    }


    public IntWritable getOutputFileIndex() {
        return outputFileIndex;
    }

    public void setOutputFileIndex(IntWritable outputFileIndex) {
        this.outputFileIndex = outputFileIndex;
    }
}
