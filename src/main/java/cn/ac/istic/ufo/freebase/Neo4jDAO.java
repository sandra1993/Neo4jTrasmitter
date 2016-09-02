package cn.ac.istic.ufo.freebase;

import org.neo4j.driver.v1.*;

/**
 * Created by ufo on 9/1/16.
 */
public class Neo4jDAO {
    private Driver driver;
    public Neo4jDAO(String path,String userName,String userPwd){
        this.setDriver(GraphDatabase.driver(path, AuthTokens.basic(userName,userPwd)));
    }

    public Driver getDriver() {
        return driver;
    }

    public void setDriver(Driver driver) {
        this.driver = driver;
    }
}
