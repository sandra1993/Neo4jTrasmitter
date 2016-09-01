package cn.ac.istic.ufo.freebase;

import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.Values;
import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.unsafe.batchinsert.BatchInserter;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.zip.GZIPInputStream;

/**
 * Load Freebase triples into a neo4j graph database.
 * Each noe4j node has a label called Entity to enable us to create schema
 * indexes over node properties e.g., __MID__.
 * Each node has two properties: __MID__, __PREFIX__.
 * Literal objects are stored as properties of the subject
 *
 * @author Abdalghani Abujabal - abujabal@mpi-inf.mpg.de
 * @version 1.0
 */


public class Neo4jBatchHandler {
    // <resource id, node id>: to keep track of inserted nodes
    private Map<String, Long> tmpIndex = new HashMap<String, Long>();

    private Neo4jDAO dao;

    public Neo4jBatchHandler(Neo4jDAO dao) {
        this.setDao(dao);
    }

    /**
     * create noe4j database
     *
     * @param path Freebase triples path
     */
    public void createNeo4jDb(String path) {
        String line = "";
        try {
            // Specifying encoding is important to store data properly
            BufferedReader br = new BufferedReader(new InputStreamReader(
                    new GZIPInputStream(new FileInputStream(path)), "UTF-8"));

            line = br.readLine();
            while (line != null) {
                String[] fields = line.split("\t");
                // subject resource
                String subjectStr = fields[0];
                Resource subjectResource = new Resource(subjectStr);

                // object resource
                String objectStr = fields[2].trim();

                Resource objectResource = new Resource(objectStr);
                // predicate resource
                String predicateStr = fields[1];
                Resource predicateResource = new Resource(predicateStr);

                if (!tmpIndex.containsKey(subjectResource.getValue()))
                    this.createNeo4jNode(subjectResource);

                if (!tmpIndex.containsKey(predicateResource.getValue()))
                    this.createNeo4jNode(predicateResource);

                if (!objectResource.isLiteral()) {
                    if (!tmpIndex.containsKey(objectResource.getValue()))
                        this.createNeo4jNode(objectResource);
                    this.createNeo4jRelation(tmpIndex.get(predicateResource.getValue()),
                            tmpIndex.get(subjectResource.getValue()),
                            tmpIndex.get(objectResource.getValue()));
                } else
                    this.createProperty(tmpIndex.get(predicateResource.getValue())
                            , tmpIndex.get(subjectResource.getValue()), objectResource);

                line = br.readLine();
            }
            br.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void createNeo4jNode(Resource resource) {
        tmpIndex.put(resource.getValue(), this.getDao().run("CREATE (n:Concept {conceptName:\""
                + resource.getValue()
                + "\"}) return ID(n);").next().get("ID(n)").asLong());
    }

    public void createNeo4jRelation(long predicateNodeId, long subjectNodeId,
                                    long objectNodeId) {
        this.getDao().run("MATCH (n:Concept),(m:Concept) where ID(n)="
                + subjectNodeId
                + " AND "
                + "ID(m)="
                + objectNodeId
                + " CREATE (n)-[:Relationship {ConceptID:"
                + predicateNodeId
                + "}]->(m) RETURN n;");
    }

    public Neo4jDAO getDao() {
        return dao;
    }

    public void setDao(Neo4jDAO dao) {
        this.dao = dao;
    }

    public void createProperty(long predicateNodeId, long subjectNodeId, Resource objectNode) {
        if (!tmpIndex.containsKey(objectNode.getValue()))
            tmpIndex.put(objectNode.getValue(), this.getDao().getSession()
                    .run("CREATE (n:Property {propertyValue:{value}}) return ID(n);",
                            Values.parameters("value", objectNode.getValue()))
                    .next().get("ID(n)").asLong());

        this.getDao().run("MATCH (n:Concept),(m:Concept) where ID(n)="
                + subjectNodeId
                + " AND "
                + "ID(m)="
                + tmpIndex.get(objectNode.getValue())
                + " CREATE (n)-[:Relationship {ConceptID:"
                + predicateNodeId
                + "}]->(m) RETURN n;");
    }
}
