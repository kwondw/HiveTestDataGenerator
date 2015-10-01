package kwon.dongwook.mapreduce;


import kwon.dongwook.error.UnsupportedFeatureException;
import kwon.dongwook.io.RangeKey;
import kwon.dongwook.io.TableInfo;
import kwon.dongwook.model.DummyDataGenerator;
import kwon.dongwook.model.Partition;
import kwon.dongwook.model.QueryParser;
import kwon.dongwook.model.Table;
import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.Context;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.util.List;

public class RangeGenerateMapper extends Mapper<LongWritable, Text, RangeKey, TableInfo> {

    private static Log LOG = LogFactory.getLog(RangeGenerateMapper.class);
    private QueryParser parser;
    private DummyDataGenerator dataGenerator;

    @Override
    public void setup(Context context) throws IOException, InterruptedException {
        parser = new QueryParser.Builder()
                .setJobConfiguration(context.getConfiguration())
                .build();
        dataGenerator = new DummyDataGenerator();
    }

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        Table table = null;
        try {
            table = parser.parse(value.toString());
        } catch (UnsupportedFeatureException unse) {
            // NO need to fail
        }

        if(table.isPartitioned()) {
          generateParitionRange(table, context);
        } else {
          generateNonPartitionRange(table, context);
        }
    }

    private void generateParitionRange(Table table, Context context) throws IOException, InterruptedException  {
          List<ColumnInfo> partitionCols = table.getPartitionCols();

          int partitionCount = table.getPartitionCount();
          int partitionKeyCount = partitionCols.size();
          int outputCountPerPartition = table.getOutputCountPerPartition();

          String[][] partitionKeys = new String[partitionKeyCount][partitionCount];
          for(int i = 0; i < partitionKeyCount; i++ ) {
            ColumnInfo info = partitionCols.get(i);
            partitionKeys[i] = dataGenerator.uniqueValuesOf(info, partitionCount);
          }

          for (int index = 0; index < outputCountPerPartition; index++) {
              for (int partitionIndex = 0; partitionIndex < partitionCount; partitionIndex++) {
                  RangeKey rangeKey = new RangeKey(index);
                  for (int depthIndex = 0; depthIndex < partitionKeyCount; depthIndex++) {
                      ColumnInfo info = partitionCols.get(depthIndex);
                      Partition part = makePartition(info, depthIndex, partitionKeys[depthIndex][partitionIndex]);
                      rangeKey.addPartition(part);
                  }
                  TableInfo tableInfo = createTableInfo(table);
                  tableInfo.setOutputFileIndex(new IntWritable(index));
                  tableInfo.setPartitionPathsBy(rangeKey.getPartitions());
                  context.write(rangeKey, tableInfo);
              }
          }
    }

    private void generateNonPartitionRange(Table table, Context context) throws IOException, InterruptedException  {
      int outputCountPerPartition = table.getOutputCountPerPartition();
      for (int index = 0; index < outputCountPerPartition; index++) {
        IntWritable intIndex = new IntWritable(index);
        RangeKey rangeKey = new RangeKey("/");
        rangeKey.setIndex(intIndex);
        TableInfo tableInfo = createTableInfo(table);
        tableInfo.setOutputFileIndex(intIndex);
        context.write(rangeKey, tableInfo);
      }
    }

    private TableInfo createTableInfo(Table table) {
        TableInfo tableInfo = new TableInfo(table.getLocation().toString());
        tableInfo.setCreationDDL(new Text(table.getCreationDDL()));
        return tableInfo;
    }

    private Partition makePartition(ColumnInfo info, int depth, String value) {
        Partition part = new Partition();
        part.setDepth(new IntWritable(depth));
        part.setType(new Text(info.getTypeName()));
        part.setKeyName(new Text(info.getInternalName()));
        part.setValue(new Text(value));
        return part;
    }

    @Override
    public void run(Context context) throws IOException, InterruptedException {
        setup(context);
        // Query file should be bigger than 1 block size, merge all lines in here
        // TODO: make input as sinlge file
        Text buffered = new Text();
        try {
            while (context.nextKeyValue()) {
                Text currentText = context.getCurrentValue();
                buffered.append(currentText.copyBytes(), 0, currentText.getLength());
                buffered.append("\n".getBytes(), 0, 1);
            }
            map(new LongWritable(0), buffered, context);
        } finally {
            cleanup(context);
        }
    }

}