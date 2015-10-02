package kwon.dongwook.io;


import kwon.dongwook.model.Partition;
import kwon.dongwook.model.Partitions;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class RangeKey implements WritableComparable<RangeKey> {

    private Partitions partitions;
    private Text baseRange;
    private IntWritable outputFileIndex;

    public RangeKey() {
        this.partitions = new Partitions();
        this.baseRange = new Text("");
        this.outputFileIndex = new IntWritable(1);
    }

    public boolean isPartitioned() {
        return !this.partitions.isEmpty();
    }

    public RangeKey(int outputFileIndex) {
        this();
        this.outputFileIndex = new IntWritable(outputFileIndex);
    }

    public RangeKey(Partitions partitions) {
        this();
        this.partitions = partitions;
    }

    public RangeKey(String range) {
      this();
      this.baseRange = new Text(range);
    }

    public void addPartition(Partition partition) {
        this.partitions.addPartition(partition);
    }


    public void addPartition(Partition partition, int partitionDepthIndex) {
        this.partitions.addPartition(partition, partitionDepthIndex);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        this.outputFileIndex.write(out);
        if(isPartitioned()) {
          this.partitions.write(out);
        } else {
          this.baseRange.write(out);
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.outputFileIndex.readFields(in);
        if(isPartitioned()) {
          this.partitions.readFields(in);
        } else {
          this.baseRange.readFields(in);
        }
    }

    @Override
    public int compareTo(RangeKey o) {
        return this.hashCode() - o.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        RangeKey range = (RangeKey) o;
        return isPartitioned()?
            partitions.equals(range.partitions):
            baseRange.equals(range.baseRange);
    }

    @Override
    public int hashCode() {
        return outputFileIndex.get() + (isPartitioned() ?
                partitions.hashCode(): baseRange.hashCode());
    }

    public Partitions getPartitions() {
        return partitions;
    }

    public Partition getPartition(int index) {
      return partitions.getParittion(index);
    }

    public void setPartitions(Partitions partitions) {
        this.partitions = partitions;
    }

    public Text getBaseRange() {
        return baseRange;
    }

    public void setBaseRange(Text text) {
        this.baseRange = text;
    }

    public void setOutputFileIndex(IntWritable outputFileIndex) { this.outputFileIndex = outputFileIndex; }
    public IntWritable getOutputFileIndex() { return this.outputFileIndex; }

}
