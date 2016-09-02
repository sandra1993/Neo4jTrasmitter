package cn.ac.istic.ufo.freebase;

import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.Values;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.LinkedBlockingQueue;
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
    private Map<String, Long> tmpIndex = new ConcurrentHashMap<String, Long>();
    private Neo4jDAO dao;

    private LinkedBlockingQueue<String> queue = new LinkedBlockingQueue<String>(10);
    private Boolean flag = true;
    private String path;

    public Neo4jBatchHandler(Neo4jDAO dao) {
        this.setDao(dao);
    }

    public void createNeo4jDb(String path) {
        this.setPath(path);
        new Thread(new Neo4jConsumer(this.getDao().getDriver().session())).start();
        new Thread(new Neo4jConsumer(this.getDao().getDriver().session())).start();
        new Thread(new Neo4jConsumer(this.getDao().getDriver().session())).start();
        new Neo4jProducer().run();
    }

    public Neo4jDAO getDao() {
        return dao;
    }

    public void setDao(Neo4jDAO dao) {
        this.dao = dao;
    }

    public void setPath(String path) {
        this.path = path;
    }


    class Neo4jProducer implements Runnable {
        @Override
        public void run() {
            String line = "";
            try {
                BufferedReader br = new BufferedReader(new InputStreamReader(
                        new GZIPInputStream(new FileInputStream(path)), "UTF-8"));

                line = br.readLine();
                while (line != null) {
                    queue.put(line);
                    line = br.readLine();
                }
                flag = false;
                br.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    class Neo4jConsumer implements Runnable {
        private Session session;
        public Neo4jConsumer(Session session){
            this.setSession(session);
        }

        public void createNeo4jNode(Resource resource) {
            synchronized (tmpIndex) {
                if (!tmpIndex.containsKey(resource.getValue()))
                    tmpIndex.put(resource.getValue(), this.getSession().run("CREATE (n:Concept {conceptName:\""
                            + resource.getValue()
                            + "\"}) return ID(n);").next().get("ID(n)").asLong());
            }
        }

        public void createNeo4jRelation(long predicateNodeId, long subjectNodeId,
                                        long objectNodeId) {
            this.getSession().run("MATCH (n:Concept),(m:Concept) where ID(n)="
                    + subjectNodeId
                    + " AND "
                    + "ID(m)="
                    + objectNodeId
                    + " CREATE (n)-[:Relationship {ConceptID:"
                    + predicateNodeId
                    + "}]->(m) RETURN n;");
        }

        public void createProperty(long predicateNodeId, long subjectNodeId, Resource objectNode) {
            synchronized (tmpIndex) {
                if (!tmpIndex.containsKey(objectNode.getValue()))
                    tmpIndex.put(objectNode.getValue(), this.getSession()
                            .run("CREATE (n:Property {propertyValue:{value}}) return ID(n);",
                                    Values.parameters("value", objectNode.getValue()))
                            .next().get("ID(n)").asLong());
            }

            this.getSession().run("MATCH (n:Concept),(m:Concept) where ID(n)="
                    + subjectNodeId
                    + " AND "
                    + "ID(m)="
                    + tmpIndex.get(objectNode.getValue())
                    + " CREATE (n)-[:Relationship {ConceptID:"
                    + predicateNodeId
                    + "}]->(m) RETURN n;");
        }

        @Override
        public void run() {
            while (flag) {
                try {
                    String line = queue.take();
                    String[] fields = line.split("\t");
                    String subjectStr = fields[0];
                    Resource subjectResource = new Resource(subjectStr);

                    String objectStr = fields[2].trim();

                    Resource objectResource = new Resource(objectStr);
                    String predicateStr = fields[1];
                    Resource predicateResource = new Resource(predicateStr);

                    this.createNeo4jNode(subjectResource);
                    this.createNeo4jNode(predicateResource);

                    if (!objectResource.isLiteral()) {
                        this.createNeo4jNode(objectResource);
                        this.createNeo4jRelation(tmpIndex.get(predicateResource.getValue()),
                                tmpIndex.get(subjectResource.getValue()),
                                tmpIndex.get(objectResource.getValue()));
                    } else
                        this.createProperty(tmpIndex.get(predicateResource.getValue())
                                , tmpIndex.get(subjectResource.getValue()), objectResource);

                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }

        public Session getSession() {
            return session;
        }

        public void setSession(Session session) {
            this.session = session;
        }
    }
}
