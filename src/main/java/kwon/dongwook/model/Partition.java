package kwon.dongwook.model;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Partition {

    private IntWritable depth;
    private Text keyName;
    private Text value;
    private Text type;


    public Partition() {

    }

    public Partition(Text keyName, Text type, Text value, IntWritable depth) {
        this.keyName = keyName;
        this.type = type;
        this.value = value;
        this.depth = depth;
    }

    public IntWritable getDepth() {
        return depth;
    }

    public void setDepth(IntWritable depth) {
        this.depth = depth;
    }

    public Text getKeyName() {
        return keyName;
    }

    public void setKeyName(Text keyName) {
        this.keyName = keyName;
    }

    public Text getType() {
        return type;
    }

    public void setType(Text type) {
        this.type = type;
    }

    public Text getValue() {
        return value;
    }

    public void setValue(Text value) {
        this.value = value;
    }

    public void readFields(DataInput in) throws IOException {
        this.depth.readFields(in);
        this.keyName.readFields(in);
        this.value.readFields(in);
        this.type.readFields(in);
    }

    public void write(DataOutput out) throws IOException {
        this.depth.write(out);
        this.keyName.write(out);
        this.value.write(out);
        this.type.write(out);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("depth=");
        if(depth.compareTo(new IntWritable(-1)) > 0)
            sb.append(depth.get());
        sb.append(",");
        if(keyName != null)
            sb.append(keyName.toString());
        sb.append("=");
        if(value != null)
            sb.append(value.toString());
        sb.append(",type=");
        if(type != null)
            sb.append(type.toString());
        return sb.toString();
    }

    @Override
    public int hashCode() {
        return toString().hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if(obj instanceof Partition) {
            return this.hashCode() == obj.hashCode();
        } else {
            return false;
        }
    }
}
