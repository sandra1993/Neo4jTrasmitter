package cn.ac.istic.ufo.freebase;

import org.junit.Test;

/**
 * Created by ufo on 8/31/16.
 */
public class Neo4jBatchHandlerTest {
    @Test
    public void parse(){
        String databaseDir="bolt://192.168.31.237";
        String userName="neo4j";
        String userPwd="ufowzh13524645";
        String freebasePath="/home/ufo/1.gz";
        Neo4jBatchHandler handler =
                new Neo4jBatchHandler(new Neo4jDAO(databaseDir, userName, userPwd));
        handler.createNeo4jDb(freebasePath);
    }
}