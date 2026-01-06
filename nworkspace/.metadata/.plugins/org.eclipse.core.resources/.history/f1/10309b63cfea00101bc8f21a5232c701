package bigdatacourse.hw2.studentcode;

import java.nio.file.Paths;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Set;
import java.util.TreeSet;
import com.datastax.oss.driver.api.core.CqlSession;

import bigdatacourse.hw2.HW2API;

public class HW2StudentAnswer implements HW2API{
	
	// general consts
	public static final String		NOT_AVAILABLE_VALUE 	=		"na";

	// CQL stuff
	//TODO: add here create table and query designs 
	
	// cassandra session
	private CqlSession session;
	
	// prepared statements
	//TODO: add here prepared statements variables
	
	
	@Override
	public void connect(String pathAstraDBBundleFile, String username, String password, String keyspace) {
		if (session != null) {
			System.out.println("ERROR - cassandra is already connected");
			return;
		}
		
		System.out.println("Initializing connection to Cassandra...");
		
		this.session = CqlSession.builder()
				.withCloudSecureConnectBundle(Paths.get(pathAstraDBBundleFile))
				.withAuthCredentials(username, password)
				.withKeyspace(keyspace)
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
		//TODO: implement this function
		System.out.println("TODO: implement this function...");
	}

	@Override
	public void initialize() {
		//TODO: implement this function
		System.out.println("TODO: implement this function...");
	}

	@Override
	public void loadItems(String pathItemsFile) throws Exception {
		//TODO: implement this function
		System.out.println("TODO: implement this function...");
	}

	@Override
	public void loadReviews(String pathReviewsFile) throws Exception {
		//TODO: implement this function
		System.out.println("TODO: implement this function...");
	}

	@Override
	public String item(String asin) {
		//TODO: implement this function
		System.out.println("TODO: implement this function...");
		
		// you should return the item's description based on the formatItem function.
		// if it does not exist, return the string "not exists"
		// example for asin B005QB09TU
		String item = "not exists";	// if not exists
		if (true) // if exists
			item = formatItem(
				"B005QB09TU",
				"Circa Action Method Notebook",
				"http://ecx.images-amazon.com/images/I/41ZxT4Opx3L._SY300_.jpg",
				new TreeSet<String>(Arrays.asList("Notebooks & Writing Pads", "Office & School Supplies", "Office Products", "Paper")),
				"Circa + Behance = Productivity. The minute-to-minute flexibility of Circa note-taking meets the organizational power of the Action Method by Behance. The result is enhanced productivity, so you'll formulate strategies and achieve objectives even more efficiently with this Circa notebook and project planner. Read Steve's blog on the Behance/Levenger partnership Customize with your logo. Corporate pricing available. Please call 800-357-9991."
			);
		
		return item;
	}
	
	
	@Override
	public Iterable<String> userReviews(String reviewerID) {
		// the order of the reviews should be by the time (desc), then by the asin
		//TODO: implement this function
		System.out.println("TODO: implement this function...");
		
		// required format - example for reviewerID A17OJCRPMYWXWV
		ArrayList<String> reviewRepers = new ArrayList<String>();
		String reviewRepr1 = formatReview(
			Instant.ofEpochSecond(1362614400),
			"B005QDG2AI",
			"A17OJCRPMYWXWV",
 			"Old Flour Child",
			5,
			"excellent quality",
			"These cartridges are excellent .  I purchased them for the office where I work and they perform  like a dream.  They are a fraction of the price of the brand name cartridges.  I will order them again!"
		);
		reviewRepers.add(reviewRepr1);

		String reviewRepr2 = formatReview(
			Instant.ofEpochSecond(1360108800),
			"B003I89O6W",
			"A17OJCRPMYWXWV",
			"Old Flour Child",
			5,
			"Checkbook Cover",
			"Purchased this for the owner of a small automotive repair business I work for.  The old one was being held together with duct tape.  When I saw this one on Amazon (where I look for almost everything first) and looked at the price, I knew this was the one.  Really nice and very sturdy."
		);
		reviewRepers.add(reviewRepr2);

		System.out.println("total reviews: " + 2);
		return reviewRepers;
	}

	@Override
	public Iterable<String> itemReviews(String asin) {
		// the order of the reviews should be by the time (desc), then by the reviewerID
		//TODO: implement this function
		System.out.println("TODO: implement this function...");
		
		// required format - example for asin B005QDQXGQ
		ArrayList<String> reviewRepers = new ArrayList<String>();
		reviewRepers.add(
			formatReview(
				Instant.ofEpochSecond(1391299200),
				"B005QDQXGQ",
				"A1I5J5RUJ5JB4B",
				"T. Taylor \"jediwife3\"",
				5,
				"Play and Learn",
				"The kids had a great time doing hot potato and then having to answer a question if they got stuck with the &#34;potato&#34;. The younger kids all just sat around turnin it to read it."
			)
		);

		reviewRepers.add(
			formatReview(
				Instant.ofEpochSecond(1390694400),
				"B005QDQXGQ",
				"\"AF2CSZ8IP8IPU\"",
				"Corey Valentine \"sue\"",
				1,
			 	"Not good",
				"This Was not worth 8 dollars would not recommend to others to buy for kids at that price do not buy"
			)
		);
		
		reviewRepers.add(
			formatReview(
				Instant.ofEpochSecond(1388275200),
				"B005QDQXGQ",
				"A27W10NHSXI625",
				"Beth",
				2,
				"Way overpriced for a beach ball",
				"It was my own fault, I guess, for not thoroughly reading the description, but this is just a blow-up beach ball.  For that, I think it was very overpriced.  I thought at least I was getting one of those pre-inflated kickball-type balls that you find in the giant bins in the chain stores.  This did have a page of instructions for a few different games kids can play.  Still, I think kids know what to do when handed a ball, and there's a lot less you can do with a beach ball than a regular kickball, anyway."
			)
		);

		System.out.println("total reviews: " + 3);
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
