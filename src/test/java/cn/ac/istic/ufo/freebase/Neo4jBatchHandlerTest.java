package cn.ac.istic.ufo.freebase;

import org.junit.Test;

/**
 * Created by ufo on 8/31/16.
 */
public class Neo4jBatchHandlerTest {
    @Test
    public void parse(){
        String databaseDir="/home/ufo/test.db";
        String freebasePath="/home/ufo/1.gz";
        Neo4jBatchHandler handler =
                new Neo4jBatchHandler(databaseDir);
        handler.createNeo4jDb(freebasePath);
    }
}