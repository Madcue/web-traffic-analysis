package com.twq.metadata;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoDatabase;
/**
 *  mongoDB的连接工具类
 */
public class MongoDB {

     //这个参数通过任务提交命令 JavaExtraOption 指定JVM参数的key为"web.metadata.mongodbAddr"的值(MongoDB host)
     private static String mogodbAddr = System.getProperty("web.metadata.mongodbAddr", "localhost");

     private static MongoClient mongoClient = new MongoClient(mogodbAddr);

     /**
      * 获取mongodb的一个数据库的链接
      *
      * @param dbName
      * @return
      */
     public static MongoDatabase getMongoDatabase(String dbName) {
          return mongoClient.getDatabase(dbName);
     }
}
