import static com.mongodb.client.model.Filters.*;
import static com.mongodb.client.model.Projections.*;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.bson.Document;
import org.bson.conversions.Bson;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;

/**
 * Program to create a collection, insert JSON objects, and perform simple
 * queries on MongoDB.
 */
public class MongoDB {

    /**
     * MongoDB database name
     */
    public static final String DATABASE_NAME = "mydb";

    /**
     * MongoDB collection name
     */
    public static final String COLLECTION_NAME = "data";

    /**
     * Mongo client connection to server
     */
    public MongoClient mongoClient;

    /**
     * Mongo database
     */
    public MongoDatabase db;

    /**
     * Main method
     *
     * @param args no arguments required
     */
    public static void main(String[] args) throws Exception {
        MongoDB qmongo = new MongoDB();
        qmongo.connect();
        // qmongo.load();
        // qmongo.loadNest();
        System.out.println(qmongo.query1(1000));
        System.out.println(qmongo.query2(32));
        System.out.println(qmongo.query2Nest(32));
        System.out.println(qmongo.query3());
        System.out.println(qmongo.query3Nest());
        System.out.println(MongoDB.toString(qmongo.query4()));
        System.out.println(MongoDB.toString(qmongo.query4Nest()));
    }

    /**
     * Connects to Mongo database and returns database object to manipulate for
     * connection.
     *
     * @return Mongo database
     */
    public MongoDatabase connect() {
        try {
            // Provide connection information to MongoDB server
            String url = "mongodb+srv://g23ai2028:g23ai2028@g23ai2028.o060y.mongodb.net/?retryWrites=true&w=majority";
            mongoClient = MongoClients.create(url);
            System.out.println("Connection to MongoDB established successfully.");
        } catch (Exception ex) {
            System.out.println("Error: Unable to establish connection to MongoDB.");
            System.out.println("Exception: " + ex);
            ex.printStackTrace();
        }
        // Provide database information to connect to
        // Note: If database does not already exist, it will be created
        // automatically.
        db = mongoClient.getDatabase(DATABASE_NAME);
        return db;
    }

    /**
     * Loads TPC-H data into MongoDB.
     *
     * @throws Exception if a file I/O or database error occurs
     */
    public void load() throws Exception {
        // Locaion Paths to my data files
        String customerFilePath = "data/customer.tbl";
        String orderFilePath = "data/order.tbl";

        // Loading customers data into MongoDB as given
        System.out.println("Loading customers...");
        List<Document> customers = loadFileToDocuments(customerFilePath, "|", "customer");
        MongoCollection<Document> customerCollection = db.getCollection("customer");
        customerCollection.insertMany(customers);
        System.out.println("Customers loaded successfully!");

        // Loading orders data into MongoDB as per asked
        System.out.println("Loading orders...");
        List<Document> orders = loadFileToDocuments(orderFilePath, "|", "orders");
        MongoCollection<Document> orderCollection = db.getCollection("orders");
        orderCollection.insertMany(orders);
        System.out.println("Orders loaded successfully!");
    }

    /**
     * Loads customer and orders TPC-H data into a single collection.
     *
     * @throws Exception if a file I/O or database error occurs
     */
    public void loadNest() throws Exception {
        // TODO: Load customer and orders data into single collection called custorders
        // TODO: Consider using insertMany() for bulk insert for faster performance

        // Paths to your data files in my local
        String customerFilePath = "data/customer.tbl";
        String orderFilePath = "data/order.tbl";

        // Loading customers and organize them into a map for nesting
        System.out.println("Loading customers...");
        List<Document> customers = loadFileToDocuments(customerFilePath, "|", "customer");

        // Loading orders and group them by customer key
        System.out.println("Loading orders...");
        List<Document> orders = loadFileToDocuments(orderFilePath, "|", "orders");

        // Creating  mapping of custkey to orders
        Map<Integer, List<Document>> ordersByCustomer = new HashMap<>();
        for (Document order : orders) {
            int custkey = order.getInteger("custkey");
            ordersByCustomer.putIfAbsent(custkey, new ArrayList<>());
            ordersByCustomer.get(custkey).add(order);
        }

        // Combining customers and their orders into a single nested document
        System.out.println("Combining customers and orders into nested documents...");
        List<Document> custorders = new ArrayList<>();
        for (Document customer : customers) {
            int custkey = customer.getInteger("custkey");
            List<Document> customerOrders = ordersByCustomer.getOrDefault(custkey, new ArrayList<>());
            customer.put("orders", customerOrders); // Nest orders into customer document for better qyery
            custorders.add(customer);
        }

        // Inserting into 'custorders' collection
        MongoCollection<Document> custordersCollection = db.getCollection("custorders");
        custordersCollection.insertMany(custorders);
        System.out.println("Nested customers and orders loaded successfully!");
    }

    /**
     * Helper method to parse a .tbl file into a list of MongoDB documents.
     *
     * @param filePath  Path to .tbl file
     * @param delimiter Delimiter used in file
     * @param type      Type of data (e.g., customer or orders) for field mapping
     * @return List of MongoDB documents
     * @throws Exception If a file I/O error occurs
     */
    private List<Document> loadFileToDocuments(String filePath, String delimiter, String type) throws Exception {
        List<Document> documents = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
            String line;
            while ((line = reader.readLine()) != null) {
                String[] fields = line.split("\\" + delimiter);

                Document document = new Document();
                if (type.equals("customer")) {
                      document.put("custkey", Integer.parseInt(fields[0].trim()));
                    document.put("name", fields[1].trim());
                    document.put("address", fields[2].trim());
                    document.put("nationkey", Integer.parseInt(fields[3].trim()));
                    document.put("phone", fields[4].trim());
                    document.put("acctbal", Double.parseDouble(fields[5].trim()));
                    document.put("mktsegment", fields[6].trim());
                    document.put("comment", fields[7].trim());
                } else if (type.equals("orders")) {
                     document.put("orderkey", Integer.parseInt(fields[0].trim()));
                    document.put("custkey", Integer.parseInt(fields[1].trim()));
                    document.put("orderstatus", fields[2].trim());
                    document.put("totalprice", Double.parseDouble(fields[3].trim()));
                    document.put("orderdate", fields[4].trim());
                    document.put("orderpriority", fields[5].trim());
                    document.put("clerk", fields[6].trim());
                    document.put("shippriority", Integer.parseInt(fields[7].trim()));
                    document.put("comment", fields[8].trim());
                }

                documents.add(document);
            }
        }
        return documents;
    }

    /**
     * Performs a MongoDB query that returns customer name given a customer id
     * using customer collection.
     *
     * @param custkey customer id (custkey) to search for.
     * @return name of customer or a message indicating no customer was
     *         found.
     */
    public String query1(int custkey) {
        System.out.println("\nExecuting query 1: Find customer name by custkey");
        // TODO: Writing query
        try {
            // Accessing 'customer' collection
            MongoCollection<Document> col = db.getCollection("customer");
            // See: https://docs.mongodb.com/drivers/java/sync/current/usage-examples/find/

            // Query collection for given custkey
            Document customer = col.find(eq("custkey", custkey)).projection(fields(include("name"), exclude("_id")))
                    .first();

            // Checking if a result was found
            if (customer != null) {
                // Returning customer name
                return customer.getString("name");
            } else {
                // No customer found with given custkey
                return "No customer found with custkey: " + custkey;
            }
        } catch (Exception ex) {
            // Handling any exceptions that occur during  query
            ex.printStackTrace();
            return "Error executing query: " + ex.getMessage();
        }
    }

    /**
     * Performs a MongoDB query that returns order date for a given order id using
     * orders collection.
     */
    public String query2(int orderId) {
        // TODO: Write a MongoDB query
        System.out.println("\nExecuting query 2: Find order date by orderId");

        try {
            // Accessing 'orders' collection
            MongoCollection<Document> col = db.getCollection("orders");

            // Query collection for given orderId
            Document order = col.find(eq("orderkey", orderId)).projection(fields(include("orderdate"), exclude("_id")))
                    .first();

            // Checking if a result was found
            if (order != null) {
                // Return order date
                return order.getString("orderdate");
            } else {
                // No order found with given orderId
                return "No order found with orderId: " + orderId;
            }
        } catch (Exception ex) {
            // Handling any exceptions that occur during query
            ex.printStackTrace();
            return "Error executing query: " + ex.getMessage();
        }
    }

    /**
     * Performs a MongoDB query that returns order date for a given order id using
     * custorders collection.
     */
    public String query2Nest(int orderId) {
        // TODO: Write a MongoDB query
        System.out.println("\nExecuting query 2 nested: Find order date by orderId in custorders");

        try {
            // Accessing 'custorders' collection
            MongoCollection<Document> col = db.getCollection("custorders");

            // Query to search for an order within nested orders array
            Document customer = col.find(eq("orders.orderkey", orderId))
                    .projection(fields(include("orders"), exclude("_id")))
                    .first();

            // Checking if a result was found
            if (customer != null) {
                // Get orders array from document
                List<Document> orders = (List<Document>) customer.get("orders");

                // Search for specific order within nested orders array
                for (Document order : orders) {
                    if (order.getInteger("orderkey") == orderId) {
                        // Return order date if found
                        return order.getString("orderdate");
                    }
                }
            }

            // No order found with given orderId
            return "No order found with orderId: " + orderId;
        } catch (Exception ex) {
            // Handling any exceptions that occur during query
            ex.printStackTrace();
            return "Error executing query: " + ex.getMessage();
        }
    }

    /**
     * Performs a MongoDB query that returns total number of orders using the
     * orders collection.
     */
    public long query3() {
        // TODO: Write a MongoDB query
        System.out.println("\nExecuting query 3: Count total number of orders");

        try {
            // Accessing 'orders' collection
            MongoCollection<Document> col = db.getCollection("orders");

            // Use countDocuments() method to count all documents in collection
            long totalOrders = col.countDocuments();

            // Return total count
            return totalOrders;
        } catch (Exception ex) {
            // Handling any exceptions that occur during query
            ex.printStackTrace();
            return -1; // Return -1 to indicate an error
        }
    }

    /**
     * Performs a MongoDB query that returns total number of orders using the
     * custorders collection.
     */
    public long query3Nest() {
        // TODO: Write a MongoDB query
        System.out.println("\nExecuting query 3 nested: Count total number of orders in custorders");

        try {
            // Accessing 'custorders' collection
            MongoCollection<Document> col = db.getCollection("custorders");

            // Using an aggregation pipeline to sum lengths of all 'orders' arrays
            List<Document> pipeline = List.of(
                    new Document("$unwind", "$orders"), // Unwind 'orders' array to process each order as a separate
                                                        // document
                    new Document("$count", "totalOrders") // Count all unwound documents
            );

            // Executing aggregation query
            Document result = col.aggregate(pipeline).first();

            // Checking if result contains total count
            if (result != null) {
                // Handling result as either Integer or Long
                Object totalOrders = result.get("totalOrders");
                if (totalOrders instanceof Integer) {
                    return ((Integer) totalOrders).longValue();
                } else if (totalOrders instanceof Long) {
                    return (Long) totalOrders;
                } else {
                    return 0; // Fallback if type is unexpected
                }
            } else {
                return 0; // Return 0 if no orders were found
            }
        } catch (Exception ex) {
            // Handling any exceptions that occur during query
            ex.printStackTrace();
            return -1; // Return -1 to indicate an error
        }
    }

    /**
     * Performs a MongoDB query that returns top 5 customers based on total
     * order amount using customer and orders collections.
     */
    public MongoCursor<Document> query4() {
        System.out.println("\nExecuting query 4: Find top 5 customers based on total order amount");

        try {
            // Accessing 'orders' and 'customer' collections
            MongoCollection<Document> ordersCol = db.getCollection("orders");
            MongoCollection<Document> customerCol = db.getCollection("customer");

            // Aggregation pipeline for 'orders' collection
            List<Document> pipeline = List.of(
                    // Group orders by customer key and calculate total order amount for each
                    // customer
                    new Document("$group", new Document("_id", "$custkey")
                            .append("totalOrderAmount", new Document("$sum", "$totalprice"))),

                    // Sort customers by total order amount in descending order
                    new Document("$sort", new Document("totalOrderAmount", -1)),

                    // Limit to top 5 customers
                    new Document("$limit", 5),

                    // Lookup customer details from 'customer' collection
                    new Document("$lookup", new Document("from", "customer")
                            .append("localField", "_id")
                            .append("foreignField", "custkey")
                            .append("as", "customerDetails")),

                    // Unwind customer details array to flatten results
                    new Document("$unwind", "$customerDetails"),

                    // ProjectING required fields
                    new Document("$project", new Document("custkey", "$_id")
                            .append("name", "$customerDetails.name")
                            .append("totalOrderAmount", 1)
                            .append("_id", 0)));

            // Executing aggregation
            return ordersCol.aggregate(pipeline).iterator();
        } catch (Exception ex) {
            // Handling any exceptions that occur during query
            ex.printStackTrace();
            return null; // Return null to indicate an error
        }
    }

    /**
     * Performs a MongoDB query that returns top 5 customers based on total
     * order amount using custorders collection.
     */
    public MongoCursor<Document> query4Nest() {
        System.out
                .println("\nExecuting query 4 nested: Find top 5 customers based on total order amount in custorders");

        try {
            // Accessing 'custorders' collection
            MongoCollection<Document> col = db.getCollection("custorders");

            // Aggregation pipeline
            List<Document> pipeline = List.of(
                    // Unwind orders array to process each order as a separate document
                    new Document("$unwind", "$orders"),

                    // Group by customer and calculate total order amount for each customer
                    new Document("$group", new Document("_id", "$custkey")
                            .append("name", new Document("$first", "$name")) // Include customer name
                            .append("totalOrderAmount", new Document("$sum", "$orders.totalprice"))),

                    // Sort by total order amount in descending order
                    new Document("$sort", new Document("totalOrderAmount", -1)),

                    // Limit to top 5 customers
                    new Document("$limit", 5),

                    // ProjectING required fields
                    new Document("$project", new Document("custkey", "$_id")
                            .append("name", 1)
                            .append("totalOrderAmount", 1)
                            .append("_id", 0)));

            // Executing aggregation
            return col.aggregate(pipeline).iterator();
        } catch (Exception ex) {
            // Handling any exceptions that occur during query
            ex.printStackTrace();
            return null; // Return null to indicate an error
        }
    }

    /**
     * Returns Mongo database being used.
     *
     * @return Mongo database
     */
    public MongoDatabase getDb() {
        return db;
    }

    /**
     * Outputs a cursor of MongoDB results in string form.
     *
     * @param cursor Mongo cursor
     * @return results as a string
     */
    public static String toString(MongoCursor<Document> cursor) {
        StringBuilder buf = new StringBuilder();
        int count = 0;
        buf.append("Rows:\n");
        if (cursor != null) {
            while (cursor.hasNext()) {
                Document obj = cursor.next();
                buf.append(obj.toJson());
                buf.append("\n");
                count++;
            }
            cursor.close();
        }
        buf.append("Number of rows: " + count);
        return buf.toString();
    }
}
