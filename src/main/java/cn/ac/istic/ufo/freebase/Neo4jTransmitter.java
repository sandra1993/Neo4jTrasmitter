package cn.ac.istic.ufo.freebase;

/**
 * Hello world!
 */
public class Neo4jTransmitter {
    public static void main(String[] args) {
        if (args.length < 4)
            System.err.println("Usage: freebasePath "
                    + "noe4jDatabasePath "
                    + "userName"
                    + "userPwd"
                    + "[#triples]");

        Neo4jBatchHandler handler =
                new Neo4jBatchHandler(new Neo4jDAO(args[1], args[2], args[3]));
        handler.createNeo4jDb(args[0]);
    }
}
