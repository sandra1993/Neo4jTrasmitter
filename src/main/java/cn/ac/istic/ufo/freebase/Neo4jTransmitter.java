package cn.ac.istic.ufo.freebase;

/**
 * Hello world!
 */
public class Neo4jTransmitter {
    public static void main(String[] args) {
        if (args.length < 2)
            System.err.println("Usage: freebasePath "
                    + "noe4jDatabasePath ");

        Neo4jBatchHandler handler =
                new Neo4jBatchHandler(args[1]);
        handler.createNeo4jDb(args[0]);
    }
}
