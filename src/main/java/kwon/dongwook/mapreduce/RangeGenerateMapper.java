package kwon.dongwook.mapreduce;


import kwon.dongwook.error.UnsupportedFeatureException;
import kwon.dongwook.io.RangeKey;
import kwon.dongwook.io.TableInfo;
import kwon.dongwook.model.*;
import org.apache.hadoop.hive.ql.exec.ColumnInfo;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.util.ArrayList;
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

  public ArrayList<Partition> clonePartitionList(ArrayList<Partition> list) {
    List<Partition> clone = new ArrayList<Partition>(list.size());
    for(Partition item: list) {
      clone.add(item.clone());
    }
    return clone;
  }

    private void generatePartitions(int currentPartitionIndex, int depthIndex, String[][] partitionKeys, ArrayList<Partition> partitionsKeyList, List<ColumnInfo> partitionCols, ArrayList<Partitions> partitionsList) {
        if(depthIndex >= (partitionKeys.length - 1)) {
            Partitions parts  = new Partitions();
            for(Partition part: partitionsKeyList) {
                parts.addPartition(part);
            }
            parts.addPartition(makePartition(partitionCols.get(depthIndex), depthIndex, partitionKeys[depthIndex][currentPartitionIndex]));
            partitionsList.add(parts);
            return;
        }
        for(int partitionIndex = 0; partitionIndex < partitionKeys[depthIndex].length; partitionIndex++) {
            ArrayList<Partition> partitionsKeyListClone = clonePartitionList(partitionsKeyList);
            partitionsKeyListClone.add(makePartition(partitionCols.get(depthIndex), depthIndex, partitionKeys[depthIndex][partitionIndex]));
            generatePartitions(partitionIndex, depthIndex + 1, partitionKeys, partitionsKeyListClone, partitionCols, partitionsList);
        }
    }

  private void generateParitionRange(Table table, Context context) throws IOException, InterruptedException  {
    List<ColumnInfo> partitionCols = table.getPartitionCols();

    int partitionCount = table.getPartitionCount();
    int partitionDepthCount = partitionCols.size();
    int outputCountPerPartition = table.getOutputCountPerPartition();

    String[][] partitionKeys = new String[partitionDepthCount][];
    for(int i = 0; i < partitionDepthCount; i++ ) {
      ColumnInfo info = partitionCols.get(i);
      partitionKeys[i] = dataGenerator.uniqueValuesOf(info, partitionCount);
    }

    ArrayList<Partitions> partitionsList = new ArrayList<Partitions>();
    generatePartitions(0, 0, partitionKeys, null, partitionCols, partitionsList);

    for(Partitions partitions: partitionsList) {
      for (int fileIndex = 0; fileIndex < outputCountPerPartition; fileIndex++) {
        RangeKey rangeKey = new RangeKey(fileIndex);
        rangeKey.setPartitions(partitions);

        TableInfo tableInfo = createTableInfo(table);
        tableInfo.setOutputFileIndex(new IntWritable(fileIndex));
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
        rangeKey.setOutputFileIndex(intIndex);
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


  private void generateParitionRange2(Table table, Context context) throws IOException, InterruptedException  {
    List<ColumnInfo> partitionCols = table.getPartitionCols();

    int partitionCount = table.getPartitionCount();
    int partitionDepthCount = partitionCols.size();
    int outputCountPerPartition = table.getOutputCountPerPartition();

    String[][] partitionKeys = new String[partitionDepthCount][];
    for(int i = 0; i < partitionDepthCount; i++ ) {
      ColumnInfo info = partitionCols.get(i);
      partitionKeys[i] = dataGenerator.uniqueValuesOf(info, partitionCount);
    }

    int lastPartitionIndex = partitionDepthCount-1;
    int lastPartitionDepth = partitionKeys[lastPartitionIndex].length;
    for (int fileIndex = 0; fileIndex < outputCountPerPartition; fileIndex++) {
      for (int partitionIndex = 0; partitionIndex < lastPartitionDepth; partitionIndex++) {
        RangeKey rangeKey = new RangeKey(fileIndex);
        // TODO : support multi depth level partition
        // It only support last depth as one level partition for now
        int partitionDepthIndex = lastPartitionIndex;
//                  for (int partitionDepthIndex = 0; partitionDepthIndex < partitionDepthCount; partitionDepthIndex++) {
        ColumnInfo info = partitionCols.get(partitionDepthIndex);
        Partition part = makePartition(info, partitionDepthIndex, partitionKeys[partitionDepthIndex][partitionIndex]);
        rangeKey.addPartition(part);
        //                 }
        TableInfo tableInfo = createTableInfo(table);
        tableInfo.setOutputFileIndex(new IntWritable(fileIndex));
        tableInfo.setPartitionPathsBy(rangeKey.getPartitions());
        context.write(rangeKey, tableInfo);
      }
    }
  }

}
