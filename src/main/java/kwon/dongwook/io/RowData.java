package kwon.dongwook.io;

import kwon.dongwook.model.Table;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

public class RowData implements WritableComparable<RowData> {



    private ArrayList<Text> values;
    private Text fieldTerminator;
    private Text escaper;
    private Text collectionTerminator;
    private Text mapKeyTerminator;
    private Text lineTerminator;
    private Text nullDefiner;
    private Table.FILE_FORMAT fileFormat;

    public RowData() {
        this.values = new ArrayList<Text>();
        collectionTerminator = new Text(Table.DEFAULT_COLLECTION_TERMINATOR);
        mapKeyTerminator = new Text(Table.DEFAULT_MAPKEY_TERMINATOR);
        lineTerminator = new Text(Table.DEFAULT_LINE_TERMINATOR);
        fieldTerminator = new Text(Table.DEFAULT_FIELD_TERMINATOR);
        fileFormat = Table.DEFAULT_FILE_FORMAT;
    }

    public RowData(Table table) {
        this();
        this.fieldTerminator = new Text(table.getFieldTerminator());
        if(table.getEscaper() != null)
            this.escaper = new Text(table.getEscaper());
        this.collectionTerminator = new Text(table.getCollectionTerminator());
        this.mapKeyTerminator = new Text(table.getMapKeyTerminator());
        this.lineTerminator = new Text(table.getLineTerminator());
        if(table.getNullDefiner() != null)
            this.nullDefiner = new Text(table.getNullDefiner());
        if(table.getOutputFileFormat() != null)
            this.fileFormat = table.getOutputFileFormat();
    }

    @Override
    public int compareTo(RowData o) {
        return this.hashCode() - o.hashCode();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        for(int i = 0; i < values.size(); i++) {
            Text value = values.get(i);
            value.write(out);
            if(!isLastIndex(i)) {
                this.fieldTerminator.write(out);
            }
        }
        if(this.fileFormat != Table.FILE_FORMAT.TEXTFILE) {
            this.lineTerminator.write(out);
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        for(int i = 0; i < values.size(); i++) {
            Text value = values.get(i);
            value.readFields(in);
            if(!isLastIndex(i)) {
                this.fieldTerminator.readFields(in);
            }
        }
        if(this.fileFormat != Table.FILE_FORMAT.TEXTFILE) {
            this.lineTerminator.readFields(in);
        }
    }

    @Override
    public int hashCode() {
        return toString().hashCode();
    }

    @Override
    public String toString() {
        if(this.values.isEmpty()) return "EMPTY";
        StringBuilder sb = new StringBuilder();
        for(int i = 0; i < values.size(); i++) {
            Text value = values.get(i);
            sb.append(value.toString());
            if(!isLastIndex(i)) {
                sb.append(this.fieldTerminator.toString());
            }
        }
        if(this.fileFormat != Table.FILE_FORMAT.TEXTFILE) {
            sb.append(this.lineTerminator.toString());
        }
        return sb.toString();
    }

    private boolean isLastIndex(int i) {
        return (i == (this.values.size() - 1));
    }

    public ArrayList<Text> getValues() {
        return values;
    }

    public void setValues(ArrayList<Text> values) {
        this.values = values;
    }

    public Text getNullDefiner() {
        return nullDefiner;
    }

    public void setNullDefiner(Text nullDefiner) {
        this.nullDefiner = nullDefiner;
    }

    public Text getEscaper() {
        return escaper;
    }

    public void setEscaper(Text escaper) {
        this.escaper = escaper;
    }

    public Text getCollectionTerminator() {
        return collectionTerminator;
    }

    public void setCollectionTerminator(Text collectionTerminator) {
        this.collectionTerminator = collectionTerminator;
    }

    public Text getMapKeyTerminator() {
        return mapKeyTerminator;
    }

    public void setMapKeyTerminator(Text mapKeyTerminator) {
        this.mapKeyTerminator = mapKeyTerminator;
    }

    public Text getLineTerminator() {
        return lineTerminator;
    }

    public void setLineTerminator(Text lineTerminator) {
        this.lineTerminator = lineTerminator;
    }

    public Text getFieldTerminator() {
        return fieldTerminator;
    }

    public void setFieldTerminator(Text fieldTerminator) {
        this.fieldTerminator = fieldTerminator;
    }

}
