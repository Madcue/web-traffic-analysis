package com.twq.metadata;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoDatabase;

public class MongoDB {
    public static String mongodbAddr = System.getProperty("web.metadata.mongodbAddr", "localhost");
    private static MongoClient mongoClient = new MongoClient(mongodbAddr);
    /**
     *  获取mongodb的一个数据库的链接
     * @param dbName
     * @return
     */
    public static MongoDatabase getMongoDatabase(String dbName){
        return mongoClient.getDatabase(dbName);
    }
}
