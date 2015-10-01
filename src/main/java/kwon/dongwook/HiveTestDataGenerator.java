package kwon.dongwook;

import kwon.dongwook.error.UnsupportedFeatureException;
import kwon.dongwook.io.RowData;
import kwon.dongwook.io.TableInfo;
import kwon.dongwook.io.RangeKey;
import kwon.dongwook.io.TableDataOutputFormat;
import kwon.dongwook.mapreduce.DataGenerateReducer;
import kwon.dongwook.mapreduce.RangeGenerateMapper;
import kwon.dongwook.model.Table;

import kwon.dongwook.model.QueryParser;
import kwon.dongwook.model.Config;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;

public class HiveTestDataGenerator extends Configured implements Tool {

    private static final Log LOG = LogFactory.getLog(HiveTestDataGenerator.class.getName());
    private static final String NA = "Not Available";
    private static int MAPPER_FACTOR = 50;

    private Options options;
    private HelpFormatter helpFormatter;
    private boolean printToolOptions = false;

    public HiveTestDataGenerator() {
        helpFormatter = new HelpFormatter();
        createOptions();
    }

    private void parseOptions(Configuration conf, String[] args) throws ParseException {
        CommandLineParser parser = new BasicParser();
        CommandLine cmd = parser.parse(options, args);
        if(!cmd.hasOption("q")) {
            throw new ParseException("--query-path does not exist!");
        }
        String queryPath = cmd.getOptionValue("q");
        int partitionCount = cmd.hasOption("p") ? ((Number) cmd.getParsedOptionValue("p")).intValue(): 0;
        String destinationPath = cmd.hasOption("d") ? cmd.getOptionValue("d"): null;
        String fileFormat = cmd.hasOption("f") ? cmd.getOptionValue("f"): null;
        int outputCountPerPartition = cmd.hasOption("c") ? ((Number) cmd.getParsedOptionValue("c")).intValue(): 1;
        int rowCountPerFile = cmd.hasOption("r") ? ((Number) cmd.getParsedOptionValue("r")).intValue(): 50;

        conf.set(Config.Vars.HIVE_QUERY_PATH.varName, queryPath);
        conf.setInt(Config.Vars.PARTITION_COUNT.varName, partitionCount);
        if (destinationPath != null)
            conf.set(Config.Vars.DESTINATION_PATH.varName, destinationPath);
        if (fileFormat != null)
            conf.set(Config.Vars.FILE_FORMAT.varName, fileFormat);
        conf.setInt(Config.Vars.OUTPUT_COUNT_PER_PARTITION.varName, outputCountPerPartition);
        conf.setInt(Config.Vars.ROW_COUNT_PER_FILE.varName, rowCountPerFile);
    }

    private void createOptions() {
        options = new Options();
        options.addOption(OptionBuilder
                .withLongOpt("query-path")
                .hasArg(true).withType(String.class)
                .isRequired(true).withValueSeparator()
                .withDescription("Hive creation DDL file on S3")
                .create('q'));
        options.addOption(OptionBuilder
                .withLongOpt("partition-count")
                .hasArg(true).withType(Number.class)
                .withValueSeparator()
                .withDescription("Number of partitions to generate")
                .create("p"));
        options.addOption("d", "destination-path", true, "Target path to generate test data");
        options.addOption("f", "file-format", true, "Output file format");
        options.addOption(OptionBuilder
                .withLongOpt("file-count-per-partition")
                .hasArg(true).withType(Number.class)
                .withValueSeparator()
                .withDescription("Output file count per partition, for no partition, total number of output files")
                .create("c"));
        options.addOption(OptionBuilder
                .withLongOpt("row-count-per-file")
                .hasArg(true).withType(Number.class)
                .withValueSeparator()
                .withDescription("the number of row per each input file to generate")
                .create("r"));
    }

    private HelpFormatter getHelpFormatter() {
        return helpFormatter;
    }

    private int printUsage() {
        System.out.println("");
        getHelpFormatter().printHelp("hiveTestDataGenerator", options);
        if (printToolOptions)
            ToolRunner.printGenericCommandUsage(System.err);
        return 0;
    }

    public int run(String[] args) throws Exception {
        if (args.length < 1) {
            return printUsage();
        }

        Configuration conf = this.getConf();
        try {
            parseOptions(conf, args);
        } catch(ParseException pe) {
            System.err.println(pe.getMessage());
            return printUsage();
        }

        Job job = createJob(conf);

        return job.waitForCompletion(true) ? 0: 1;
    }

    private Job createJob(Configuration conf) throws IOException {
        Job job = Job.getInstance(conf, "Hive Test Data Generator");
        job.setJarByClass(HiveTestDataGenerator.class);
        job.setMapperClass(RangeGenerateMapper.class);
        job.setReducerClass(DataGenerateReducer.class);
        job.getConfiguration().set(JobContext.MAP_SPECULATIVE, "false");
        job.getConfiguration().set(JobContext.REDUCE_SPECULATIVE, "false");

        String hiveQueryPathString = job.getConfiguration().get(Config.Vars.HIVE_QUERY_PATH.varName);
        Path hiveQueryPath = new Path(hiveQueryPathString);

        QueryParser parser = new QueryParser.Builder()
                .setHiveQueryPath(hiveQueryPath)
                .setJobConfiguration(job.getConfiguration())
                .build();

        Table table = null;
        try {
            table = parser.parse();
        } catch (UnsupportedFeatureException use) {
            LOG.error("Unsupported feature : " + use.getMessage());
            System.err.println(use.getMessage());
            System.exit(use.getReasonCode());
        }

        int partitionCount = table.getPartitionCount();
        int outputCountPerPartition = table.getOutputCountPerPartition();
        if (partitionCount == 0)
            partitionCount = 1;
        int ranger = (int)Math.floor(partitionCount / MAPPER_FACTOR) + 1;
        LOG.info("Setting " + (partitionCount * outputCountPerPartition)
                + " reducers for " + partitionCount + " partitionCount * " + outputCountPerPartition + " outputCountPerPartition" );
        LOG.info("Setting " + ranger + " mapper");

        job.setNumReduceTasks(partitionCount * outputCountPerPartition);
        job.getConfiguration().set(JobContext.NUM_MAPS, String.valueOf(ranger));
        job.setInputFormatClass(TextInputFormat.class);

        job.setMapOutputKeyClass(RangeKey.class);
        job.setMapOutputValueClass(TableInfo.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(RowData.class);
        job.setOutputFormatClass(LazyOutputFormat.class);

        LazyOutputFormat.setOutputFormatClass(job, TableDataOutputFormat.class);
        FileInputFormat.addInputPath(job, hiveQueryPath);
        FileOutputFormat.setOutputPath(job, new Path(table.getLocation()));

        return job;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = 333;
        try {
            exitCode = ToolRunner.run(new Configuration(), new HiveTestDataGenerator(), args);
        } catch (Exception e) {
            LOG.error("Failed : ", e);
        }
        System.exit(exitCode);
    }
}
