package kwon.dongwook.model;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.SemanticException;

public abstract class HiveSemanticAnalyzer extends BaseSemanticAnalyzer {

    public HiveSemanticAnalyzer(HiveConf conf) throws SemanticException {
        super(conf);
    }

    public static String getTypeStringFromAST(ASTNode typeNode) throws SemanticException {
        return BaseSemanticAnalyzer.getTypeStringFromAST(typeNode);
    }
}
