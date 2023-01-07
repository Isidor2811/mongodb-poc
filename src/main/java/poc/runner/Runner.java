package poc.runner;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.in;
import static poc.db.DBCollections.MOVIES;
import static poc.db.DBCollections.THEATERS;
import static poc.db.DBCollections.USERS;

import com.github.javafaker.Faker;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.FindIterable;
import com.mongodb.client.ListCollectionsIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Accumulators;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Updates;
import com.mongodb.client.result.UpdateResult;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;
import org.bson.Document;
import org.bson.conversions.Bson;
import poc.db.DataBase;
import poc.db.dto.Theater;
import poc.db.dto.Theater.Address;
import poc.db.dto.Theater.Geo;
import poc.db.dto.Theater.Location;
import poc.db.dto.User;

public class Runner {

  public static void main(String[] args) {

    MongoDatabase dataBase = DataBase.getDataBase();
    MongoCollection<Document> usersCollection = dataBase.getCollection(USERS);
    MongoCollection<Document> theatersCollection = dataBase.getCollection(THEATERS);
    MongoCollection<Theater> theatersCollectionForPojo = dataBase.getCollection(THEATERS,
        Theater.class);
    MongoCollection<Document> moviesCollection = dataBase.getCollection(MOVIES);

    //SELECT ALL
    FindIterable<Document> findIterable = usersCollection.find(new Document());
    for (Document document : findIterable) {
      System.out.println(document.getString("name"));
    }

    //FILTER BY EQUAL
    Bson equalByName = eq("name", "Jaime Lannister");
    FindIterable<Document> users = usersCollection.find(equalByName);
    users.forEach((Consumer<? super Document>) doc -> System.out.println(doc.toJson()));

    //FILTER BY EQUAL #2
    Bson equalByNestedFields = eq("location.address.city", "San Diego");
    FindIterable<Document> theaters = theatersCollection.find(equalByNestedFields);
    theaters.forEach((Consumer<? super Document>) doc -> System.out.println(doc.toJson()));

    //FILTER WITH AND
    Bson andFilter = and(eq("location.address.city", "San Diego"),
        eq("location.address.zipcode", "92108"));
    FindIterable<Document> andFilterDocuments = theatersCollection.find(andFilter);
    andFilterDocuments.forEach(
        (Consumer<? super Document>) doc -> System.out.println(doc.toJson()));

    //ARRAYS
    Bson arrayFilter = Filters.eq("cast", Arrays.asList("Charles Kayser", "John Ott"));
    Document movieDocument = moviesCollection.find(arrayFilter).first();
    System.out.println(Objects.requireNonNull(movieDocument).getString("title"));

    //find movies where in cast array exactly 4 elements
    Bson castOfThreeFilter = Filters.size("cast", 4);
    FindIterable<Document> documents = moviesCollection.find(castOfThreeFilter);
    for (Document document : documents) {
      System.out.println(document.getString("title"));
    }

    //IN operator
    Bson inFilter = in("countries", Arrays.asList("Germany", "Canada"));
    FindIterable<Document> inDocuments = moviesCollection.find(inFilter);
    for (Document inDocument : inDocuments) {
      System.out.println(inDocument.get("countries"));
    }

    //INSERT
    Faker faker = new Faker();
    String fullName = faker.name().fullName();
    String emailAddress = faker.internet().emailAddress();
    Document userToInsert = new Document("name", fullName)
        .append("email", emailAddress)
        .append("password", faker.internet().password());
    usersCollection.insertOne(userToInsert);
    //get inserted element
    System.out.println(Objects.requireNonNull(usersCollection.find(eq("email", emailAddress))
        .first()).getString("name"));

    //UPDATE
    UpdateResult updateResult = usersCollection.updateOne(eq("name", fullName),
        Updates.set("email", "lalallal@test.com"));
    System.out.println(updateResult.getModifiedCount());

    //DELETE
    System.out.println(usersCollection.deleteOne(eq("name", fullName))
        .getDeletedCount());

    //AGGREGATION (count)
    Document countAggregation = moviesCollection
        .aggregate(Arrays.asList(
            Aggregates.match(eq("rated", "UNRATED")),
            Aggregates.count()
        )).first();
    System.out.println(Objects.requireNonNull(countAggregation).get("count"));

    //AGGREGATION (group)
    AggregateIterable<Document> aggregate = moviesCollection
        .aggregate(List.of(
            Aggregates.group("$type", Accumulators.sum("type", 1))
        ));
    for (Document document : aggregate) {
      System.out.println(document.toJson());
    }

    //SELECT AS POJO
    FindIterable<User> usersAsPOJO = usersCollection.find(new Document(), User.class);
    for (User user : usersAsPOJO) {
      System.out.println(user.getName());
    }

    //INSERT AS POJO
    Address address = Address.builder()
        .zipcode("4242-33")
        .state("UKR")
        .street1("Pavlova,2")
        .city("Kyiv").build();
    List<Double> geoCoordinates = Arrays.asList(3231.3, -3993.1);

    Geo geo = Geo.builder()
        .coordinates(geoCoordinates)
        .type("Rec").build();

    Location location = Location.builder()
        .address(address)
        .geo(geo)
        .build();

    Theater theater = Theater.builder()
        .theaterId(faker.random().nextInt(1, 9999))
        .location(location).build();

    theatersCollectionForPojo.insertOne(theater);

    String street = Objects.requireNonNull(
        theatersCollectionForPojo.find(eq("location.address.city", address.getCity()))
            .first()).getLocation().getAddress().getStreet1();
    System.out.println(street);

    //UPDATE AS POJO
    geoCoordinates = Arrays.asList(1111.1, -2222.2);
    Bson updateValue = Updates.set("location.geo.coordinates", geoCoordinates);
    updateResult = theatersCollectionForPojo.updateOne(eq("location.address.state", "UKR"),
        updateValue);
    System.out.println(updateResult.getModifiedCount());

  }
}
