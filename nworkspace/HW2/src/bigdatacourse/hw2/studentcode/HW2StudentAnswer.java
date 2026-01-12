
package bigdatacourse.hw2.studentcode;

import java.nio.file.Paths;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Set;
import java.util.TreeSet;
import com.datastax.oss.driver.api.core.CqlSession;
import org.json.JSONObject;

//json
import java.io.BufferedReader;
import java.io.FileReader;
import org.json.JSONArray;

//threads
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.Semaphore;

//remove if timeout isnt needed
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import java.time.Duration;

import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
//import java.util.HashSet;
//import java.util.Set;

import bigdatacourse.hw2.HW2API;

public class HW2StudentAnswer implements HW2API{
	
	// general consts
	public static final String NOT_AVAILABLE_VALUE = "na";

	// CQL stuff
	private static final String TABLE_ITEMS = "items";
	private static final String TABLE_REVIEWS_BY_USER = "reviews_by_user";
	private static final String TABLE_REVIEWS_BY_ITEM = "reviews_by_item";
	
	// cassandra session
	private CqlSession session;
	
	// prepared statements
	private PreparedStatement insertItem;
	private PreparedStatement selectItem;
	private PreparedStatement insertReviewByUser;
	private PreparedStatement insertReviewByItem;
	private PreparedStatement selectReviewsByUser;
	private PreparedStatement selectReviewsByItem;
	
	@Override
	public void connect(String pathAstraDBBundleFile, String username, String password, String keyspace) {
		if (session != null) {
			System.out.println("ERROR - cassandra is already connected");
			return;
		}
		
		System.out.println("Initializing connection to Cassandra...");
		
		// Create a config loader to increase timeouts
	    DriverConfigLoader loader = DriverConfigLoader.programmaticBuilder()
	            .withDuration(DefaultDriverOption.REQUEST_TIMEOUT, Duration.ofSeconds(10))
	            .withDuration(DefaultDriverOption.CONNECTION_INIT_QUERY_TIMEOUT, Duration.ofSeconds(10))
	            .withDuration(DefaultDriverOption.CONTROL_CONNECTION_TIMEOUT, Duration.ofSeconds(10))
	            .build();
	    
		this.session = CqlSession.builder()
				.withCloudSecureConnectBundle(Paths.get(pathAstraDBBundleFile))
				.withAuthCredentials(username, password)
				.withKeyspace(keyspace)
				.withConfigLoader(loader)
				.build();
		
		System.out.println("Initializing connection to Cassandra... Done");
	}


	@Override
	public void close() {
		if (session == null) {
			System.out.println("Cassandra connection is already closed");
			return;
		}
		
		System.out.println("Closing Cassandra connection...");
		session.close();
		System.out.println("Closing Cassandra connection... Done");
	}

	
	
	@Override
	public void createTables() {
		//items table
		String createItemsTable = 
		"CREATE TABLE IF NOT EXISTS items (" +
	            "    asin text," +
	            "    category_name text," +
	            "    title text static," +
	            "    description text static," +
	            "    imUrl text static," +
	            "    PRIMARY KEY (asin, category_name)" +
	            ") WITH CLUSTERING ORDER BY (category_name ASC);";
		
		session.execute(createItemsTable);
		
		//reviews_by_user table 
		String createReviewsByUserTable = 
			    "CREATE TABLE IF NOT EXISTS " + TABLE_REVIEWS_BY_USER + " (" +
			    "    asin text," +
			    "    time timestamp," +
			    "    reviewerID text," +
			    "    reviewerName text," +
			    "    rating int," +
			    "    summary text," +
			    "    reviewText text," +
			    "    PRIMARY KEY (reviewerID, time, asin)" +
			    ") WITH CLUSTERING ORDER BY (time DESC, asin ASC);";	
		
		session.execute(createReviewsByUserTable);
		
		//reviews_by_item table
		String createReviewsByItemTable = 
			    "CREATE TABLE IF NOT EXISTS " + TABLE_REVIEWS_BY_ITEM + " (" +
			    "    asin text," +
			    "    time timestamp," +
			    "    reviewerID text," +
			    "    reviewerName text," +
			    "    rating int," +
			    "    summary text," +
			    "    reviewText text," +
			    "    PRIMARY KEY ((asin), time, reviewerID)" +
			    ") WITH CLUSTERING ORDER BY (time DESC, reviewerID ASC);";
		
		session.execute(createReviewsByItemTable);
	}

	@Override
	public void initialize() {
		//items
		insertItem = session.prepare("INSERT INTO " + TABLE_ITEMS + " (asin, category_name, title, description, imUrl) VALUES (?, ?, ?, ?, ?)");
	    selectItem = session.prepare("SELECT * FROM " + TABLE_ITEMS + " WHERE asin = ?");

	    //reviews
	    insertReviewByUser = session.prepare("INSERT INTO " + TABLE_REVIEWS_BY_USER + " (reviewerID, time, asin, reviewerName, rating, summary, reviewText) VALUES (?, ?, ?, ?, ?, ?, ?)");
	    insertReviewByItem = session.prepare("INSERT INTO " + TABLE_REVIEWS_BY_ITEM + " (asin, time, reviewerID, reviewerName, rating, summary, reviewText) VALUES (?, ?, ?, ?, ?, ?, ?)");
	    
	    selectReviewsByUser = session.prepare("SELECT * FROM " + TABLE_REVIEWS_BY_USER + " WHERE reviewerID = ?");
	    selectReviewsByItem = session.prepare("SELECT * FROM " + TABLE_REVIEWS_BY_ITEM + " WHERE asin = ?");
    	
	}
	@Override
	public void loadItems(String pathItemsFile) throws Exception {
		// initialize 250 threads
		ExecutorService executor = Executors.newFixedThreadPool(250);
		// allow only 100 concurrent requests to AstraDB at a time
		Semaphore throttler = new Semaphore(100);

		try (BufferedReader br = new BufferedReader(new FileReader(pathItemsFile))) {
			String line;
			while ((line = br.readLine()) != null) {
				final String currentLine = line;
				
				executor.execute(() -> {
					try {
						throttler.acquire(); // wait for permit to go
						
						JSONObject json = new JSONObject(currentLine);
						String asin  = json.optString("asin", NOT_AVAILABLE_VALUE);
						String title = json.optString("title", NOT_AVAILABLE_VALUE);
						String image = json.optString("imUrl", NOT_AVAILABLE_VALUE);
						String desc  = json.optString("description", NOT_AVAILABLE_VALUE);
						
						// check if categories are not empty
						if (json.has("categories") && json.getJSONArray("categories").length() != 0) {
							JSONArray categoriesOuter = json.getJSONArray("categories");
							for (int i = 0; i < categoriesOuter.length(); i++) {
								JSONArray categoriesInner = categoriesOuter.getJSONArray(i);
								for (int j = 0; j < categoriesInner.length(); j++) {
									String categoryName = categoriesInner.optString(j, NOT_AVAILABLE_VALUE);
									
									// Flattening categories 
									session.execute(insertItem.bind(asin, categoryName, title, desc, image));
								}
							}
						} else {
							// fallback to NA if categories are missing or empty
							session.execute(insertItem.bind(asin, NOT_AVAILABLE_VALUE, title, desc, image));
						}
					} catch (Exception e) {
					} finally {
						throttler.release();
					}
				});
			}
		} catch (Exception e) {
			System.out.println("Loading items failed: " + e.getMessage());
		}
		
		executor.shutdown();
		executor.awaitTermination(1, TimeUnit.HOURS);
		System.out.println("Done loading items");
	}

	@Override
	public void loadReviews(String pathReviewsFile) throws Exception {
		ExecutorService executor = Executors.newFixedThreadPool(250);
		Semaphore throttler = new Semaphore(100);

		try (BufferedReader br = new BufferedReader(new FileReader(pathReviewsFile))) {
			String line;
			while ((line = br.readLine()) != null) {
				final String currentLine = line;
				
				executor.execute(() -> {
					try {
						throttler.acquire();
						
						JSONObject json = new JSONObject(currentLine);
						
						// Extract attributes 
						String asin         = json.optString("asin", NOT_AVAILABLE_VALUE);
						String reviewerID   = json.optString("reviewerID", NOT_AVAILABLE_VALUE);
						String reviewerName = json.optString("reviewerName", NOT_AVAILABLE_VALUE);
						String summary      = json.optString("summary", NOT_AVAILABLE_VALUE);
						String reviewText   = json.optString("reviewText", NOT_AVAILABLE_VALUE);
						int rating          = json.optInt("overall", -1); 
						
						long unixTime = json.optLong("unixReviewTime", 0);
						Instant time = Instant.ofEpochSecond(unixTime);
						
						session.execute(insertReviewByItem.bind(asin, time, reviewerID, reviewerName, rating, summary, reviewText));
						session.execute(insertReviewByUser.bind(reviewerID, time, asin, reviewerName, rating, summary, reviewText));

					} catch (Exception e) {
					} finally {
						throttler.release();
					}
				});
			}
		} catch (Exception e) {
			System.out.println("Loading reviews failed: " + e.getMessage());
		}
				
		executor.shutdown();
		executor.awaitTermination(1, TimeUnit.HOURS);
		System.out.println("Done loading reviews");
	}
	

	@Override
	public String item(String asin) {
		// execute query
	    com.datastax.oss.driver.api.core.cql.ResultSet rs = session.execute(selectItem.bind(asin));
	    java.util.List<com.datastax.oss.driver.api.core.cql.Row> rows = rs.all();

	    // return "not exists" if the asin isn't found
	    if (rows.isEmpty()) {
	        return "not exists";
	    }

	    // extract all columns that do not change between same ansi rows
	    com.datastax.oss.driver.api.core.cql.Row firstRow = rows.get(0);
	    String title = firstRow.getString("title");
	    String imageUrl = firstRow.getString("imUrl");
	    String description = firstRow.getString("description");

	    // get all categories into a TreeSet
	    Set<String> categories = new TreeSet<>();
	    for (com.datastax.oss.driver.api.core.cql.Row row : rows) {
	        String category = row.getString("category_name");
	        
	        // filter out the 'na' placeholder we used
	        if (category != null && !category.equals(NOT_AVAILABLE_VALUE)) {
	            categories.add(category);
	        }
	    }
	    
	    return formatItem(asin, title, imageUrl, categories, description);
	}
	
	
	@Override
	public Iterable<String> userReviews(String reviewerID) {
		// list to store the formatted review strings
	    ArrayList<String> reviewRepers = new ArrayList<String>();
	    
		// execute query - results arrive pre-sorted by Cassandra (time DESC, asin ASC)
	    com.datastax.oss.driver.api.core.cql.ResultSet rs = session.execute(selectReviewsByUser.bind(reviewerID));
	    
		// iterate through all reviews found for this user
	    for (com.datastax.oss.driver.api.core.cql.Row row : rs) {
	        String reviewRepr = formatReview(
	            row.getInstant("time"),
	            row.getString("asin"),
	            row.getString("reviewerID"),
	            row.getString("reviewerName"),
	            row.getInt("rating"),
	            row.getString("summary"),
	            row.getString("reviewText")
	        );
	        reviewRepers.add(reviewRepr);
	    }

	    // print the total count and return the list
	    System.out.println("total reviews: " + reviewRepers.size());
	    return reviewRepers;
	}

	@Override
	public Iterable<String> itemReviews(String asin) {
		// list to store the formatted review strings
		ArrayList<String> reviewRepers = new ArrayList<String>();
			
		// execute query - results arrive pre-sorted by Cassandra (time DESC, reviewerID ASC)
		ResultSet rs = session.execute(selectReviewsByItem.bind(asin));
			
		// iterate through all reviews found for this item
		for (Row row : rs) {
			String reviewRepr = formatReview(
				row.getInstant("time"),
				row.getString("asin"),
				row.getString("reviewerID"),
				row.getString("reviewerName"),
				row.getInt("rating"),
				row.getString("summary"),
				row.getString("reviewText")
			);
			reviewRepers.add(reviewRepr);
		}

		System.out.println("total reviews: " + reviewRepers.size());
		return reviewRepers;	
	}

	
	
	// Formatting methods, do not change!
	private String formatItem(String asin, String title, String imageUrl, Set<String> categories, String description) {
		String itemDesc = "";
		itemDesc += "asin: " + asin + "\n";
		itemDesc += "title: " + title + "\n";
		itemDesc += "image: " + imageUrl + "\n";
		itemDesc += "categories: " + categories.toString() + "\n";
		itemDesc += "description: " + description + "\n";
		return itemDesc;
	}

	private String formatReview(Instant time, String asin, String reviewerId, String reviewerName, Integer rating, String summary, String reviewText) {
		String reviewDesc = 
			"time: " + time + 
			", asin: " 	+ asin 	+
			", reviewerID: " 	+ reviewerId +
			", reviewerName: " 	+ reviewerName 	+
			", rating: " 		+ rating	+ 
			", summary: " 		+ summary +
			", reviewText: " 	+ reviewText + "\n";
		return reviewDesc;
	}

}
