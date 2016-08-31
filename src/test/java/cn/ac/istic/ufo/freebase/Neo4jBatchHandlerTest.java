package cn.ac.istic.ufo.freebase;

import org.junit.Test;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserters;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

/**
 * Created by ufo on 8/31/16.
 */
public class Neo4jBatchHandlerTest {
    @Test
    public void parse(){
        String databaseDir="/home/ufo/neo";
        int numberOfTriples=-1;
        String freebasePath="/home/ufo/1.gz";
        BatchInserter db = null;
        try {
            // set these configuration based on the size of your data
            Map<String, String> config = new HashMap<String, String>();
            config.put("dbms.pagecache.memory", "70G");
            config.put("cache_type", "none");
            config.put("use_memory_mapped_buffers", "true");
            config.put("neostore.nodestore.db.mapped_memory", "10g");
            config.put("neostore.relationshipstore.db.mapped_memory", "10g");
            config.put("neostore.propertystore.db.mapped_memory", "5g");
            config.put("neostore.propertystore.db.strings.mapped_memory", "1g");
            config.put("neostore.propertystore.db.arrays.mapped_memory", "0M");
            config.put("neostore.propertystore.db.index.keys.mapped_memory", "1g");
            config.put("neostore.propertystore.db.index.mapped_memory", "10g");
            db = BatchInserters.inserter(new File(databaseDir), config);

            Neo4jBatchHandler handler = new Neo4jBatchHandler(db);
            handler.createNeo4jDb(freebasePath, numberOfTriples);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            db.shutdown();
        }
    }
}