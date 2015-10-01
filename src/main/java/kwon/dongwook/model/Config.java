package kwon.dongwook.model;


import org.apache.hadoop.conf.Configuration;

public class Config {

    public static enum Vars {
        HIVE_QUERY_PATH("data.gen.hive.query.path"),
        PARTITION_COUNT("data.gen.partition.count"),
        DESTINATION_PATH("data.gen.destination.path"),
        FILE_FORMAT("data.gen.file.format"),
        OUTPUT_COUNT_PER_PARTITION("data.gen.output.count-per-partition"),
        ROW_COUNT_PER_FILE("data.gen.output.row-count-per-file");

        public final String varName;

        Vars(String varName) {
            this.varName = varName;
        }
    }


}
