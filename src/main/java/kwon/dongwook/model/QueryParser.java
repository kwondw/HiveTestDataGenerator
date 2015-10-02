package kwon.dongwook.model;

import org.apache.commons.lang.StringEscapeUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.parse.*;

import java.io.BufferedReader;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import kwon.dongwook.Util;
import kwon.dongwook.error.UnsupportedFeatureException;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;

public class QueryParser {

    private static final Log LOG = LogFactory.getLog(QueryParser.class.getName());

    private Path queryFilePath;
    private Configuration jobConfig;
    private Context context;
    private ASTSearcher searcher;
    private String sessionID;
    private String creationDDL;

    public QueryParser(Builder builder) {
        this.queryFilePath = builder.queryFilePath;
        this.jobConfig = builder.jobConfig;
        this.context = builder.context;
        if(this.context == null) {
            HiveConf hiveConf = new HiveConf(jobConfig.getClass());
            String sessionID = "HiveDummyDataGenerator_" + UUID.randomUUID();
            hiveConf.set("_hive.hdfs.session.path", sessionID);
            hiveConf.set("_hive.local.session.path", sessionID);
            hiveConf.set("hive.support.quoted.identifiers", "column");

            this.context = new Context(hiveConf, sessionID);
        }
        this.searcher = new ASTSearcher();
    }

    public Table parseFrom(Path queryFilePath) throws UnsupportedFeatureException, IOException {
        this.queryFilePath = queryFilePath;
        return parse();
    }

    public Table parse() throws UnsupportedFeatureException, IOException {
        String ddl = download(this.queryFilePath);
        return parse(ddl);
    }

    public Table parse(String query) throws UnsupportedFeatureException, IOException {
        this.creationDDL = query;
        String statement = query.split(";")[0];

        ASTNode ast = makeAst(statement);
        LOG.info("AST created : " + ast.dump());

        ASTNode createTableNode = getCreateTableAfterValidate(ast);
        Table table = generateTableMetaDta(createTableNode, this.creationDDL);
        return table;
    }

    private ASTNode getCreateTableAfterValidate(ASTNode node) throws UnsupportedFeatureException {
        LOG.info("Validating AST nodes");
        List<ASTNode> createTableNodes = searcher.findAll(node, HiveParser.TOK_CREATETABLE);
        if (createTableNodes.size() != 1) {
            throw  new UnsupportedFeatureException("Only supports one table creation at once");
        }
        ASTNode createTableNode = createTableNodes.get(0);
        if(createTableNode.getType() != HiveParser.TOK_CREATETABLE) {
            throw new UnsupportedFeatureException("It only supports CREATE TABLE DDL");
        }
        if(!searcher.contains(createTableNode, HiveParser.KW_EXTERNAL)) {
            throw new UnsupportedFeatureException("It only supports EXTERNAL TABLE DDL");
        }
        if(searcher.contains(createTableNode, HiveParser.TOK_ALTERTABLE_BUCKETS)) {
            throw new UnsupportedFeatureException("Bucketed table is not supported by this version");
        }
        if(searcher.contains(createTableNode, HiveParser.TOK_FILEFORMAT_GENERIC)) {
           ASTNode format = searcher.findFirst(createTableNode, HiveParser.TOK_FILEFORMAT_GENERIC);
            if(!format.getChild(0).getText().equals("TEXTFILE")) {
                LOG.info("Only TEXTFILE format is supported, " + format.getChild(0).getText() + " format will be ignored");
            }
        } else if(searcher.contains(createTableNode, HiveParser.TOK_TABLEFILEFORMAT)) {
            LOG.info("Only TEXTFILE format is supported, Other format will be ignored");
        }
        return createTableNode;
    }

    private Table generateTableMetaDta(ASTNode node, String creationDDL) throws UnsupportedFeatureException {
        ASTNode name = searcher.findFirst(node, HiveParser.TOK_TABNAME);

        String tableName = BaseSemanticAnalyzer.getUnescapedUnqualifiedTableName(name);

        boolean isExternal = searcher.contains(node, HiveParser.KW_EXTERNAL);
        Table.FILE_FORMAT fileFormat = Table.FILE_FORMAT.TEXTFILE;

        Table.Builder tableBuilder = new Table.Builder(tableName)
                .setCreationDDL(creationDDL)
                .setConfig(jobConfig)
                .setHiveContext(context)
                .setExternal(isExternal)
                .setInputFileFormat(fileFormat)
                .setOutputFileFormat(fileFormat);

        ASTNode locationNode = searcher.findFirst(node, HiveParser.TOK_TABLELOCATION);
        if(locationNode != null) {
            String location = Util.removeQuote(locationNode.getChild(0).getText());
            tableBuilder.setLocation(location);
        }

        setRowformat(tableBuilder, searcher.findFirst(node, HiveParser.TOK_TABLEROWFORMAT));
        setTableCols(tableBuilder, searcher.findFirst(node, HiveParser.TOK_TABCOLLIST), tableName);
        ASTNode partitionNode = searcher.findFirst(node, HiveParser.TOK_TABLEPARTCOLS);
        if(partitionNode != null) {
            setParition(tableBuilder, partitionNode, tableName);
        } else {
            tableBuilder.setPartitionCount(0);
        }

        Table table = tableBuilder.build();
        return table;
    }

    private void setTableCols(Table.Builder tableBuilder, ASTNode tableCols, String tableName) {
        List<ASTNode> nodes = searcher.findAll(tableCols, HiveParser.TOK_TABCOL);
        if(nodes.isEmpty()) {
            return;
        }
        List<ColumnInfo> colList = getColumInfo(nodes, tableName);
        tableBuilder.setCols(colList);
    }

    private void setParition(Table.Builder tableBuilder, ASTNode partitionCols, String tableName) {
        List<ASTNode> nodes = searcher.findAll(partitionCols, HiveParser.TOK_TABCOL);
        if(nodes.isEmpty()) {
            return;
        }
        List<ColumnInfo> colList = getColumInfo(nodes, tableName);
        tableBuilder.setPartitionCols(colList);
    }

    private List<ColumnInfo> getColumInfo(List<ASTNode> colNodes, String tableName) {
        List<ColumnInfo> cols = new ArrayList<ColumnInfo>();
        for (ASTNode colNode : colNodes) {
            ASTNode typeNode = (ASTNode)colNode.getChild(1);
            String colAlias = BaseSemanticAnalyzer.unescapeIdentifier(
                                ((ASTNode)colNode.getChild(0)).getText()).toLowerCase();
            TypeInfo type = null;
            try {
                String typeString = HiveSemanticAnalyzer.getTypeStringFromAST(typeNode);
                type = TypeInfoUtils.getTypeInfoFromTypeString(typeString);
            } catch (SemanticException ex) {
                System.err.println("Failed to get type for " + colAlias + ": " + ex.getMessage() + " for " + typeNode.dump());
                return null;
            }
            cols.add(new ColumnInfo(colAlias, type, tableName, false));
        }
        return cols;
    }

    private String parseSeparator(String original) {
        String withoutQuote = Util.removeQuote(original);
        String unescape = StringEscapeUtils.unescapeJava(withoutQuote);
        try {
            Integer code = Integer.parseInt(unescape, 16);
            return Character.toString((char)code.byteValue());
        } catch (NumberFormatException nfe) {
            return unescape;
        }
    }

    private void setRowformat(Table.Builder tableBuilder, ASTNode rowFormatNode) {
        if(rowFormatNode != null) {
            ASTNode serdePropsNode = searcher.findFirst(rowFormatNode, HiveParser.TOK_SERDEPROPS);
            if(serdePropsNode != null) {
                ASTNode rowFormatFieldNode = searcher.findFirst(serdePropsNode, HiveParser.TOK_TABLEROWFORMATFIELD);
                ArrayList<Node> children = rowFormatFieldNode.getChildren();
                if(children.size() == 2) {
                    tableBuilder.setEscaper(Util.removeQuote(((ASTNode) children.get(1)).getText()));
                } else if(children.size() < 3 && !children.isEmpty()) {
                    tableBuilder.setFieldTerminator(parseSeparator(((ASTNode) children.get(0)).getText()));
                }
                ASTNode rowFormatLinesNode = searcher.findFirst(serdePropsNode, HiveParser.TOK_TABLEROWFORMATLINES);
                if(rowFormatLinesNode != null) {
                    tableBuilder.setLineTerminator(Util.removeQuote(rowFormatLinesNode.getChild(0).getText()));
                }
                ASTNode rowFormatCollection = searcher.findFirst(serdePropsNode, HiveParser.TOK_TABLEROWFORMATCOLLITEMS);
                if(rowFormatCollection != null) {
                    tableBuilder.setCollectionTerminator(Util.removeQuote(rowFormatCollection.getChild(0).getText()));
                }
                ASTNode rowFormatMap = searcher.findFirst(serdePropsNode, HiveParser.TOK_TABLEROWFORMATMAPKEYS);
                if(rowFormatMap != null) {
                    tableBuilder.setMapKeyTerminator(Util.removeQuote(rowFormatMap.getChild(0).getText()));
                }
                ASTNode rowFormatNull = searcher.findFirst(serdePropsNode, HiveParser.TOK_TABLEROWFORMATNULL);
                if(rowFormatNull != null) {
                    tableBuilder.setNullDefiner(Util.removeQuote(rowFormatNull.getChild(0).getText()));
                }
            }
        }
    }

    private ASTNode makeAst(String statement) throws IOException {
        LOG.debug("Making AST from : [" + statement + "]");
        ASTNode ast = null;
        try {
            ParseDriver pd = new ParseDriver();
            ast = pd.parse(statement, context);
        } catch (ParseException e) {
            System.err.println("Parsing failed for " + statement);
            e.printStackTrace(System.err);
            System.exit(997);
        } catch (NullPointerException npe) {
            System.err.println("Parsing failed by NPE for " + statement);
            npe.printStackTrace();
            System.exit(998);
        }
        return ast;
    }

    public String download(Path path) {
        FSDataInputStream input = null;
        String query = null;
        try {
            FileSystem sourceFS = path.getFileSystem(this.jobConfig);
            input = sourceFS.open(path);
            StringBuilder sb = new StringBuilder();
            BufferedReader reader = new BufferedReader(new InputStreamReader(input));
            String line;
            while( (line = reader.readLine()) != null ) {
                sb.append(line).append(" ");
            }
            query = sb.toString();
            System.out.println("Reading file : " + query);
        } catch (EOFException eofe) {
            eofe.printStackTrace();
        } catch (IOException ioe) {
            ioe.printStackTrace();
        } finally {
            try {
                if(input != null)
                    input.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
            return query;
        }
    }

    public static class Builder {

        private Configuration jobConfig;
        private Path queryFilePath;
        private Context context;

        public Builder() {

        }

        public Builder setJobConfiguration(Configuration jobConfig) {
            this.jobConfig = jobConfig;
            return this;
        }

        public Builder setHiveQueryPath(Path path) {
            this.queryFilePath = path;
            return this;
        }

        public Builder setContext(Context context) {
            this.context = context;
            return this;
        }

        public QueryParser build() {
            return new QueryParser(this);
        }
    }
}
