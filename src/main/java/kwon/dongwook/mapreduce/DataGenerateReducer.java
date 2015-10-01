package kwon.dongwook.mapreduce;


import kwon.dongwook.error.UnsupportedFeatureException;
import kwon.dongwook.io.RangeKey;
import kwon.dongwook.io.RowData;
import kwon.dongwook.io.TableInfo;
import kwon.dongwook.model.Config;
import kwon.dongwook.model.DummyDataGenerator;
import kwon.dongwook.model.QueryParser;
import kwon.dongwook.model.Table;
import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.ReduceContext;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


public class DataGenerateReducer extends Reducer<RangeKey, TableInfo, NullWritable, RowData> {

    private static Log LOG = LogFactory.getLog(DataGenerateReducer.class);

    private Table table = null;
    private MultipleOutputs<NullWritable, RowData> multipleOutputs;
    private QueryParser parser = null;
    private int rowCountPerFile = 1;
    private DummyDataGenerator dataGenerator;
    private int outputCountPerPartition = 1;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        multipleOutputs = new MultipleOutputs<NullWritable, RowData>(context);
        parser = new QueryParser.Builder()
                .setJobConfiguration(context.getConfiguration())
                .build();
        dataGenerator = new DummyDataGenerator();
      this.outputCountPerPartition = context.getConfiguration().getInt(Config.Vars.OUTPUT_COUNT_PER_PARTITION.varName, outputCountPerPartition);
    }

    @Override
    public void reduce(RangeKey rangeKey, Iterable<TableInfo> tableInfos, Context context) throws IOException, InterruptedException {
        boolean tableCreated = false;
        Table table  = null;
        Iterator<TableInfo> itr = tableInfos.iterator();
        while(itr.hasNext()) {
          TableInfo tableInfo = itr.next();
          if (!tableCreated) {
            table = getTable(tableInfo);
            tableCreated = true;
            LOG.info("Writing output into " + tableInfo.getOutputPath());
          }
          writeRowData(tableInfo, table);
        }
    }

    private void writeRowData(TableInfo tableInfo, Table table) throws IOException, InterruptedException {
        rowCountPerFile = table.getRowCountPerFile();
        int fileIndex = tableInfo.getOutputFileIndex().get();
        for(int i = 0; i < rowCountPerFile; i++) {
            RowData row = createRow(table);
            multipleOutputs.write(NullWritable.get(), row, filePath(tableInfo, fileIndex));
        }
    }

    private RowData createRow(Table table) {
        RowData row = new RowData(table);
        List<ColumnInfo> colsInfo = table.getCols();
        Iterator<ColumnInfo> itr = colsInfo.iterator();
        while(itr.hasNext()) {
            ColumnInfo info = itr.next();
            String value = dataGenerator.generateValue(info);
            row.getValues().add(new Text(value));
        }
        return row;
    }

    private synchronized Table getTable(TableInfo tableInfo) throws IOException {
        if (this.table == null) {
            try {
                this.table = parser.parse(tableInfo.getCreationDDL().toString());
            } catch (UnsupportedFeatureException usfe) {
                // No need to faile
            }
        }
        return table;
    }

    private String filePath(TableInfo tableInfo, int fileIndex) {
      String index = (fileIndex > 0) ? Integer.toString(fileIndex): "";
      String prefix = tableInfo.isPartitioned() ? "/partition": "output";
      return tableInfo.getOutputPath().toString() + prefix + index;
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        multipleOutputs.close();
    }
}
