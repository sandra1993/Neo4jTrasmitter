package cn.ac.istic.ufo.freebase;

import org.neo4j.graphdb.*;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.zip.GZIPInputStream;

public class Neo4jBatchHandler {
    private Map<String, Node> tmpIndex = new ConcurrentHashMap<String, Node>();
    private GraphDatabaseService graphDb;

    private LinkedBlockingQueue<String> queue = new LinkedBlockingQueue<String>(1000);
    private Boolean flag = true;
    private String path;

    protected final Label CONCEPT = Label.label("Concept");
    protected final Label PROPERTY = Label.label("Property");
    protected final RelationshipType RELATIONSHIP = RelationshipType.withName("Relationship");

    public Neo4jBatchHandler(String neo4jPath) {
        this.setGraphDb(new GraphDatabaseFactory().newEmbeddedDatabase(new File(neo4jPath)));
    }

    public void createNeo4jDb(String path) {
        this.setPath(path);
        Thread[] threads = new Thread[3];
        for (int i = 0; i < threads.length; i++) {
            threads[i] = new Thread(new Neo4jConsumer());
            threads[i].start();
        }

        new Neo4jProducer().run();
        int count = 0;
        while (count < threads.length) {
            count = 0;
            for (int i = 0; i < threads.length; i++)
                count += !threads[i].isAlive() ? 1 : 0;
        }
        graphDb.shutdown();
    }

    public void setPath(String path) {
        this.path = path;
    }

    public GraphDatabaseService getGraphDb() {
        return graphDb;
    }

    public void setGraphDb(GraphDatabaseService graphDb) {
        this.graphDb = graphDb;
    }


    class Neo4jProducer implements Runnable {
        @Override
        public void run() {
            String line = "";
            try {
                BufferedReader br = new BufferedReader(new InputStreamReader(
                        new GZIPInputStream(new FileInputStream(path)), "UTF-8"));

                line = br.readLine();
                int c = 0;
                while (line != null) {
                    queue.put(line);
                    line = br.readLine();
                    c++;
                    if (c > 2000)
                        break;
                }
                flag = false;
                br.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    class Neo4jConsumer implements Runnable {
        public Neo4jConsumer() {
        }

        public void createNeo4jNode(Resource resource) {
            synchronized (tmpIndex) {
                Node node;
                if (!tmpIndex.containsKey(resource.getValue()))
                    try (Transaction tx = graphDb.beginTx()) {
                        node = graphDb.createNode();
                        node.addLabel(CONCEPT);
                        node.setProperty("conceptName", resource.getValue());
                        tmpIndex.put(resource.getValue(), node);
                        tx.success();
                    }
            }
        }

        public void createNeo4jRelation(Node predicateNode, Node subjectNode,
                                        Node objectNode) {
            try (Transaction tx = graphDb.beginTx()) {
                Relationship relationship = subjectNode.createRelationshipTo(objectNode, RELATIONSHIP);
                relationship.setProperty("ConceptId", predicateNode.getId());
                tx.success();
            }
        }

        public void createProperty(Node predicateNode, Node subjectNode, Resource objectNode) {
            Node node;
            synchronized (tmpIndex) {
                if (!tmpIndex.containsKey(objectNode.getValue()))
                    try (Transaction tx = graphDb.beginTx()) {
                        node = graphDb.createNode();
                        node.addLabel(PROPERTY);
                        node.setProperty("propertyValue", objectNode.getValue());
                        tmpIndex.put(objectNode.getValue(), node);
                        tx.success();
                    }
            }
            node = tmpIndex.get(objectNode.getValue());

            try (Transaction tx = graphDb.beginTx()) {
                Relationship relationship = subjectNode.createRelationshipTo(node, RELATIONSHIP);
                relationship.setProperty("ConceptId", predicateNode.getId());
                tx.success();
            }
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
    }
}
