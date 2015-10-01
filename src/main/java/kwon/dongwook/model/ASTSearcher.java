package kwon.dongwook.model;


import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveParser;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

public class ASTSearcher {

    private final LinkedList<ASTNode> queue;

    public ASTSearcher() {
        this.queue = new LinkedList<ASTNode>();
    }

    private void resetQueueWith(ASTNode ast) {
        queue.clear();
        queue.add(ast);
    }

    public List<ASTNode> findAll(ASTNode ast, int tokenType) {
        resetQueueWith(ast);
        List<ASTNode> results = new ArrayList<ASTNode>();
        while(!queue.isEmpty()) {
            ASTNode next = queue.poll();
            if (next.getType() == tokenType) {
                results.add(next);
            }
            for (int i = 0; i < next.getChildCount(); i++) {
                queue.add((ASTNode) next.getChild(i));
            }
        }
        return results;
    }

    public ASTNode findFirst(ASTNode ast, int tokenType) {
        return (ASTNode) ast.getFirstChildWithType(tokenType);
    }

    public boolean contains(ASTNode ast, int tokenType) {
        ASTNode node = findFirst(ast, tokenType);
        return (node != null);
    }
}
