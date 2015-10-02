package kwon.dongwook.model;


import org.apache.hadoop.io.IntWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

public class Partitions {

    private ArrayList<Partition> partitions;

    public Partitions() {
        this(new ArrayList<Partition>());
    }

    public Partitions(ArrayList<Partition> partitions) {
        this.partitions = partitions;
    }

    public void addPartition(Partition partition) {
        this.partitions.add(partition);
    }

  public void addPartition(Partition partition, int depthIndex) {
    partition.setDepth(depthIndex);
    this.partitions.add(depthIndex, partition);
  }

    public ArrayList<Partition> getPartitions() {
        return partitions;
    }

    public Partition getParittion(int index) {
      return partitions.get(index);
    }

    public void setPartitions(ArrayList<Partition> partitions) {
        this.partitions = partitions;
    }

    public boolean isEmpty() {
        return partitions.isEmpty();
    }

    public void readFields(DataInput in) throws IOException {
        Iterator<Partition> itr = partitions.iterator();
        while(itr.hasNext()) {
            Partition part = itr.next();
            part.readFields(in);
        }
    }

    public void write(DataOutput out) throws IOException {
        Iterator<Partition> itr = partitions.iterator();
        while(itr.hasNext()) {
            Partition part = itr.next();
            part.write(out);
        }
    }

    @Override
    public String toString() {
        if(partitions == null || partitions.isEmpty())
            return "NO_PARTITION";
        StringBuilder sb = new StringBuilder();
        Iterator<Partition> iter = partitions.iterator();
        while(iter.hasNext()) {
            Partition part = iter.next();
            sb.append(part.toString()).append(":");
        }
        return sb.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Partitions that = (Partitions) o;
        if (partitions == null || partitions.isEmpty()) {
            return that.getPartitions().isEmpty();
        }
        return toString().equals(o.toString());
    }

    @Override
    public int hashCode() {
        if(partitions == null || partitions.isEmpty())
            return 0;
        return toString().hashCode();
    }
}
