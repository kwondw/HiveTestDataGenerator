package kwon.dongwook.model;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.Context;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.List;

public class Table {

    public static enum FILE_FORMAT {
        TEXTFILE, ORC, SEQUENCEFILE, RCFILE, PARQUET, AVRO
    }

    public final static String DEFAULT_LINE_TERMINATOR = "\n";
    public final static String DEFAULT_FIELD_TERMINATOR = "\001";
    public final static String DEFAULT_COLLECTION_TERMINATOR = "\002";
    public final static String DEFAULT_MAPKEY_TERMINATOR = "\003";
    public final static FILE_FORMAT DEFAULT_FILE_FORMAT = FILE_FORMAT.TEXTFILE;

    private static final Log LOG = LogFactory.getLog(Table.class.getName());

    private String name;
    private String location;
    private int partitionCount;
    private FILE_FORMAT inputFileFormat;
    private FILE_FORMAT outputFileFormat;
    private String fieldTerminator;
    private String escaper;
    private String collectionTerminator;
    private String mapKeyTerminator;
    private String lineTerminator;
    private String nullDefiner;
    private List<ColumnInfo> cols;
    private boolean isExternal = true;
    private List<ColumnInfo> partitionCols;
    private int outputCountPerPartition;
    private Configuration config;
    private Context hiveContext;
    private int rowCountPerFile;
    private String creationDDL;

    private Table() throws Exception {
        throw new Exception("Please use Builder class");
    }

    public Table(Builder builder) {
        Class builderClass = builder.getClass();
        Class tableClass = this.getClass();
        Field[] fields = builderClass.getDeclaredFields();
        for (Field field : fields) {
            try {
                Field name = tableClass.getDeclaredField(field.getName());
                if (field.get(builder) != null) {
                    name.set(this, field.get(builder));
                }
            } catch (IllegalAccessException iae) {
                LOG.info(iae.getMessage());
            } catch (NoSuchFieldException nsfe) {
                LOG.info("No " + field + " found! " + nsfe.getMessage());
            }
        }
        this.partitionCount = config.getInt(Config.Vars.PARTITION_COUNT.varName, partitionCount);
        if(partitionCols == null || partitionCols.isEmpty())
          this.partitionCount = 0;
        this.location = config.get(Config.Vars.DESTINATION_PATH.varName, location);
//        String strOutputFormat = config.get(Config.Vars.FILE_FORMAT.varName);
        this.outputCountPerPartition = config.getInt(Config.Vars.OUTPUT_COUNT_PER_PARTITION.varName, outputCountPerPartition);
        this.rowCountPerFile = config.getInt(Config.Vars.ROW_COUNT_PER_FILE.varName, rowCountPerFile);
    }

    public boolean isPartitioned() {
        return !(this.partitionCols == null || this.partitionCols.isEmpty());
    }

    public String getName() {
        return name;
    }


    public String getCreationDDL() {
        return creationDDL;
    }

    public void setCreationDDL(String creationDDL) {
        this.creationDDL = creationDDL;
    }



    public int getRowCountPerFile() {
        return rowCountPerFile;
    }

    public void setRowCountPerFile(int rowCountPerFile) {
        this.rowCountPerFile = rowCountPerFile;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Context getHiveContext() {
        return hiveContext;
    }

    public void setHiveContext(Context hiveContext) {
        this.hiveContext = hiveContext;
    }

    public Configuration getConfig() {
        return config;
    }

    public void setConfig(Configuration config) {
        this.config = config;
    }

    public String getLocation() {
        return location;
    }

    public void setLocation(String location) {
        this.location = location;
    }

    public int getPartitionCount() {
        return partitionCount;
    }

    public void setPartitionCount(int partitionCount) {
        this.partitionCount = partitionCount;
    }

    public FILE_FORMAT getInputFileFormat() {
        return inputFileFormat;
    }

    public void setInputFileFormat(FILE_FORMAT inputFileFormat) {
        this.inputFileFormat = inputFileFormat;
    }

    public FILE_FORMAT getOutputFileFormat() {
        return outputFileFormat;
    }

    public void setOutputFileFormat(FILE_FORMAT outputFileFormat) {
        this.outputFileFormat = outputFileFormat;
    }

    public String getFieldTerminator() {
        return fieldTerminator;
    }

    public void setFieldTerminator(String fieldTerminator) {
        this.fieldTerminator = fieldTerminator;
    }

    public String getEscaper() {
        return escaper;
    }

    public void setEscaper(String escaper) {
        this.escaper = escaper;
    }

    public String getCollectionTerminator() {
        return collectionTerminator;
    }

    public void setCollectionTerminator(String collectionTerminator) {
        this.collectionTerminator = collectionTerminator;
    }

    public String getMapKeyTerminator() {
        return mapKeyTerminator;
    }

    public void setMapKeyTerminator(String mapKeyTerminator) {
        this.mapKeyTerminator = mapKeyTerminator;
    }

    public String getLineTerminator() {
        return lineTerminator;
    }

    public void setLineTerminator(String lineTerminator) {
        this.lineTerminator = lineTerminator;
    }

    public String getNullDefiner() {
        return nullDefiner;
    }

    public void setNullDefiner(String nullDefiner) {
        this.nullDefiner = nullDefiner;
    }

    public List<ColumnInfo> getCols() {
        return cols;
    }

    public void setCols(List<ColumnInfo> cols) {
        this.cols = cols;
    }

    public boolean isExternal() {
        return isExternal;
    }

    public void setIsExternal(boolean isExternal) {
        this.isExternal = isExternal;
    }

    public List<ColumnInfo> getPartitionCols() {
        return partitionCols;
    }

    public void setPartitionCols(List<ColumnInfo> partitionCols) {
        this.partitionCols = partitionCols;
    }


    public int getOutputCountPerPartition() {
        return outputCountPerPartition;
    }

    public void setOutputCountPerPartition(int outputCountPerPartition) {
        this.outputCountPerPartition = outputCountPerPartition;
    }



    public String toString() {
        StringBuilder sb = new StringBuilder("\nTable[\n");
        Class tableClass = this.getClass();
        Field[] builderFields = Builder.class.getDeclaredFields();

        for (Field builderField : builderFields) {
            sb.append("\t").append(builderField.getName()).append(": ");
            try {
                Field tableField = tableClass.getDeclaredField(builderField.getName());
                if (tableField.get(this) == null) {
                    sb.append("NULL");
                } else {
                    sb.append(tableField.get(this).toString());
                }
            } catch (IllegalAccessException iae) {
                sb.append("Illegal Access to ").append(builderField);
            } catch (NoSuchFieldException nsfe) {
                sb.append("No ").append(builderField).append(" exists!");
            }
            sb.append("\n");
        }

        sb.append("]");
        return sb.toString();
    }

    public static class Builder {

        protected String name;
        protected String location;
        protected int partitionCount = 0;
        protected FILE_FORMAT inputFileFormat;
        protected FILE_FORMAT outputFileFormat;
        protected String fieldTerminator;
        protected String escaper;
        protected String collectionTerminator;
        protected String mapKeyTerminator;
        protected String lineTerminator;
        protected String nullDefiner;
        protected List<ColumnInfo> cols;
        protected boolean isExternal = true;
        protected List<ColumnInfo> partitionCols;
        protected Configuration config;
        protected Context hiveContext;
        protected int outputCountPerPartition = 1;
        protected int rowCountPerFile = 50;
        protected String creationDDL;

        public Builder() {
            this.inputFileFormat = FILE_FORMAT.TEXTFILE;
            this.outputFileFormat = FILE_FORMAT.TEXTFILE;
            this.lineTerminator = DEFAULT_LINE_TERMINATOR;
            this.fieldTerminator = DEFAULT_FIELD_TERMINATOR;
            this.collectionTerminator = DEFAULT_COLLECTION_TERMINATOR;
            this.mapKeyTerminator = DEFAULT_MAPKEY_TERMINATOR;
        }

        public Builder(String name) {
            this();
            this.name = name;
        }

        public Table build() {
            Table table = new Table(this);
            return table;
        }

        public Builder setRowCountPerFile(int count) {
            this.rowCountPerFile = count;
            return this;
        }

        public Builder setOutputCountPerPartition(int count) {
            this.outputCountPerPartition = count;
            return this;
        }

        public Builder setHiveContext(Context context) {
            this.hiveContext = context;
            return this;
        }

        public Builder setConfig(Configuration config) {
            this.config = config;
            return this;
        }

        public Builder setName(String name) {
            this.name = name;
            return this;
        }

        public Builder setLocation(String location) {
            this.location = location;
            return this;
        }

        public Builder setPartitionCount(int count) {
            this.partitionCount = count;
            return this;
        }

        public Builder setExternal(boolean isExternal) {
            this.isExternal = isExternal;
            return this;
        }

        public Builder setInputFileFormat(FILE_FORMAT inputFileFormat) {
            this.inputFileFormat = inputFileFormat;
            return this;
        }

        public Builder setFieldTerminator(String terminator) {
            this.fieldTerminator = terminator;
            return this;
        }

        public Builder setOutputFileFormat(FILE_FORMAT outputFileFormat) {
            this.outputFileFormat = outputFileFormat;
            return this;
        }

        public Builder setPartitionCols(List<ColumnInfo> partitionCols) {
            this.partitionCols = partitionCols;
            return this;
        }

        public Builder setEscaper(String escaper) {
            this.escaper = escaper;
            return this;
        }

        public Builder setCollectionTerminator(String collectionTerminator) {
            this.collectionTerminator = collectionTerminator;
            return this;
        }

        public Builder setMapKeyTerminator(String mapKeyTerminator) {
            this.mapKeyTerminator = mapKeyTerminator;
            return this;
        }

        public Builder setLineTerminator(String lineTerminator) {
            this.lineTerminator = lineTerminator;
            return this;
        }

        public Builder setNullDefiner(String nullDefiner) {
            this.nullDefiner = nullDefiner;
            return this;
        }

        public Builder setCols(List<ColumnInfo> cols) {
            this.cols = cols;
            return this;
        }

        public Builder setCreationDDL(String ddl) {
            this.creationDDL = ddl;
            return this;
        }

    }
}
