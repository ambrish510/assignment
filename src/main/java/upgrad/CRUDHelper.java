package upgrad;

import com.mongodb.client.AggregateIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.Accumulators;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.Sorts;
import org.bson.Document;
import java.sql.*;
import java.util.Arrays;
import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;


public class CRUDHelper {

    /**
     * Import the Data into MongoDB from MySQL remote server
     */
    public static void importDataIntoMongoDB(
            Connection sqlConnection,
            Statement statement,
            String TableName,
            String Category,
            MongoCollection<Document> collection) throws SQLException {

        try{
            String sql = "select * from " + TableName;
            ResultSet resultSet = statement.executeQuery(sql);
            ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
            int colCount = resultSetMetaData.getColumnCount();

            System.out.println("############### Importing the data from MySQL table " + TableName + " into Mongo Collection Products Started ###############");
            System.out.println();

            //Inserting the table records into Mongo DB collection as document one by one
            while(resultSet.next()){
                Document doc = new Document();
                for(int i=1;i<=colCount;i++){
                    doc.append(
                            resultSetMetaData.getColumnLabel(i),resultSet.getString(resultSetMetaData.getColumnLabel(i)))
                            .append("Category",Category);
                }

                collection.insertOne(doc);
                System.out.println("Document inserted successfully into Mongo Collection products with Product Id: " + doc);
                System.out.println();
            }

            System.out.println("############### Importing the data from MySQL table " + TableName + " into Mongo Collection Products ended ###############");
            System.out.println();
            System.out.println();
            System.out.println();

        }
       catch(SQLException e){
            System.out.println("Got Exception.");
            e.printStackTrace();
       }
    }

    /**
     * Display ALl products
     * @param collection
     */
    public static void displayAllProducts(MongoCollection<Document> collection) {
        System.out.println("------ Displaying All Products ------");
        // Call printSingleCommonAttributes to display the attributes on the Screen
        MongoCursor<Document> cursor = collection.find().cursor();
        while(cursor.hasNext()){
            Document doc = cursor.next();
            PrintHelper.printSingleCommonAttributes(doc);
        }

    }

    /**
     * Display top 5 Mobiles
     * @param collection
     */
    public static void displayTop5Mobiles(MongoCollection<Document> collection) {
        System.out.println("------ Displaying Top 5 Mobiles ------");
        // Call printAllAttributes to display the attributes on the Screen
        MongoCursor<Document> cursor = collection.find(eq("Category","Mobile")).limit(5).cursor();
        while(cursor.hasNext()){/**
         * Display number of products in each group
         * @param collection
         */
            public static void displayProductCountByCategory(MongoCollection<Document> collection) {
                System.out.println("------ Displaying Product Count by categories ------");
                // Call printProductCountInCategory to display the attributes on the Screen
                AggregateIterable<Document> Itr = collection.aggregate(
                        Arrays.asList(
                                Aggregates.group("$Category", Accumulators.sum("Count",1))
                        )
                );

                MongoCursor<Document> cursor = Itr.cursor();

                while(cursor.hasNext()){
                    Document doc = cursor.next();
                    PrintHelper.printProductCountInCategory(doc
                    );
                }
            }
            Document doc = cursor.next();
            PrintHelper.printAllAttributes(doc);
        }
    }

    /**
     * Display products ordered by their categories in Descending order without auto generated Id
     * @param collection
     */
    public static void displayCategoryOrderedProductsDescending(MongoCollection<Document> collection) {
        System.out.println("------ Displaying Products ordered by categories ------");
        // Call printAllAttributes to display the attributes on the Screen
        MongoCursor<Document> cursor = collection.find().sort(Sorts.descending("Category")).projection(Projections.excludeId()).cursor();
        while(cursor.hasNext()){
            Document doc = cursor.next();
            PrintHelper.printAllAttributes(doc);
        }
    }

    /**
     * Display Wired Headphones
     * @param collection
     */
    public static void displayWiredHeadphones(MongoCollection<Document> collection) {
        System.out.println("------ Displaying Wired headphones ------");
        // Call printAllAttributes to display the attributes on the Screen
        MongoCursor<Document> cursor = collection.find(and(eq("Category","Headphone"),eq("ConnectorType","Wired"))).cursor();
        while(cursor.hasNext()){
            Document doc = cursor.next();
            PrintHelper.printAllAttributes(doc);
        }
    }
}