package poc.db;

import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
import static org.bson.codecs.configuration.CodecRegistries.fromRegistries;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoException;
import com.mongodb.client.MongoDatabase;
import lombok.extern.slf4j.Slf4j;
import org.bson.BsonDocument;
import org.bson.BsonInt64;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.PojoCodecProvider;
import org.bson.conversions.Bson;

@Slf4j
public class DataBase {

  private static final String HOST = "localhost";
  private static final int PORT = 2717;
  public static MongoClient mongoClient;
  private static final String DB_NAME = "moviesdb";
  private static MongoDatabase database;

  //create ability to get and insert data as POJO
  private static final CodecRegistry pojoCodecRegistry = fromRegistries(
      MongoClientSettings.getDefaultCodecRegistry(),
      fromProviders(PojoCodecProvider.builder().automatic(true).build()));


  //get client (pool of connections)
  public static MongoClient getMongoClient() {
    if (mongoClient == null) {
      mongoClient = new MongoClient(HOST, PORT);
      Runtime.getRuntime().addShutdownHook(new Thread(DataBase::closeInstance));
    }
    return mongoClient;
  }

  private static void closeInstance() {
    mongoClient.close();
  }

  public static MongoDatabase getDataBase() {
    if (database == null) {
      database = getMongoClient().getDatabase(DB_NAME).withCodecRegistry(pojoCodecRegistry);
      try {
        Bson command = new BsonDocument("ping", new BsonInt64(1));
        database.runCommand(command);
        log.info("Connection to DB was successful");
      } catch (MongoException e) {
        log.info("An error occurred while attempting to run a command: " + e);
      }
    }
    return database;
  }
}
