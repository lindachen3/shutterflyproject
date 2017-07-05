import java.io.FileNotFoundException;
import java.io.FileReader;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

// the json jar file can be downloaded at http://www.java2s.com/Code/JarDownload/java/java-json.jar.zip
import org.json.*;

public class Shutterfly {
	
	class Customer {
		public String customer_id;
		
		// customer life starting date
		public Date startDate;
		
		// total expenditures
		public double expenditures;
		
		// total visits
		public int visits;
		
		public double ltv;
		
		public String last_name;
		public String adr_city;
		public String adr_state;
		
		public Customer(String id){
			customer_id = id;
			startDate = null;
			expenditures = 0.0;
			visits = 1;
			ltv = 0.0;
		}
	}
	
	class Order {
		public Date orderDate;
		public double price;
		
		public Order(Date dt, double p){
			orderDate = dt;
			price = p;
		}
	}
	
	// key = customer id; value = customer
	HashMap<String, Customer> customerProfile = new HashMap<String, Customer>();
	
	// key = order id; value = order
	HashMap<String, Order> orderHistory = new HashMap<String, Order>();
	
	// the latest date mentioned among all the events
	Date latestDate = null;
	
	public Shutterfly(){
	}
	
	//
	// Ingest an event, which may involve user profile creation/update, order creation/update, etc.
	//
	public boolean Ingest(String event){
		try{
			JSONObject obj = new JSONObject(event);
			String t = (String) obj.get("type");
			String v = (String) obj.get("verb");
			String k = (String) obj.get("key");
			
			String involvedCustomerId = k;
			
			SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", Locale.US);
			format.setTimeZone(TimeZone.getTimeZone("UTC"));
			Date et = format.parse((String) obj.get("event_time"));
			
			if(latestDate == null || et.after(latestDate)) {
				latestDate = et;
			}
			
			switch (t) {
	         case "CUSTOMER":        	 
        		 // Since the order of the events is not guaranteed, the customer information may already exist, even
	        	 // if the current event verb is "NEW"
        		 if(customerProfile.containsKey(k)){
        			 customerProfile.get(k).visits++;
        		 }
        		 else{
        			 Customer c = new Customer(k);
        			 c.startDate = et;
        			 customerProfile.put(k, c);
        		 }

        		 // if the current event is "NEW", we also need to update the customer start date information
    			 if(v.equals("NEW")){
    				 Customer c = customerProfile.get(k);
    				 c.startDate = et;
    				 c.last_name = (String) obj.get("last_name");
    				 c.adr_city = (String) obj.get("adr_city");
    				 c.adr_state = (String) obj.get("adr_state");
    			 }

	             break;

	         case "ORDER":
	         case "SITE_VISIT":
	         case "IMAGE":
	        	 
	        	 involvedCustomerId = (String) obj.get("customer_id");
	        	 Customer user = null;
	        	 
	        	 // in case the customer information does not exist, e.g., a new customer just registered, but the 
	        	 // registration event has not arrived yet as described in the project requirement.		        	 
        		 if(!customerProfile.containsKey(involvedCustomerId)){
        			 user = new Customer(involvedCustomerId);
        			 user.startDate = et;
        			 customerProfile.put(involvedCustomerId, user);
        		 }
        		 else{
        			 user = customerProfile.get(involvedCustomerId);
        			 user.visits++;
        		 }
        		 
        		 // if the event is about an order, we also need to update user expenditures accordingly
		         if(t.equals("ORDER")){
	        		 // here we assume the amount is always in usd
		        	 String amount = (String) obj.get("total_amount");
		        	 if(amount != null){
			        	 amount = amount.trim();
			        	 int sep = amount.indexOf(' ');
			        	 
			        	 Double exp = 0.0;
			        	 try{
			        		 exp = Double.parseDouble(amount.substring(0, sep > 0 ? sep : amount.length()));
			        	 }
			        	 catch(NumberFormatException e){
			        		 e.printStackTrace();
			        		 break;
			        	 }
			        	 
		        		 // I assume that the order update event share the same "key" with the original order event;
			        	 // otherwise, there is not enough information to track the order
			        	 if(orderHistory.containsKey(k)){
		        			 Order prevOrder = orderHistory.get(k);
		        			 
		        			 // if the current order event is newer, we need to update the record.
		        			 // Note that the original order event ("NEW") may arrive later than some of the related order update events
		        			 if(v != "NEW" && et.after(prevOrder.orderDate)){
			        			 Double diff = exp - prevOrder.price;
			        			 prevOrder.orderDate = et;
			        			 prevOrder.price = exp;
			        			 
			        			 // we compute the price difference in order to update the user expenditures.
			        			 exp = diff;
			        		 }
			        		 // otherwise, discard the current event which is outdated
			        		 else{
			        			 exp = 0.0;
			        		 }
		        		 }
		        		 // we receive an order event, which can be either a completely new order or an order update
		        		 else{
		        			 orderHistory.put(k, new Order(et, exp));
		        		 }
			        	 
			        	 // update user expenditures information
	        			 user.expenditures += exp;
		        	 }
		         }
	        	 break;
	         default:
	             break;
			}
			
			return true;
		}
		catch (JSONException | ParseException e) {
			e.printStackTrace();
			return false;
		}
	}
	
	//
	// Compute and update the LTV for a customer at given date
	//
	private void updateLTV(Customer user, Date currentDate) {
		long diff = Math.abs(currentDate.getTime() - user.startDate.getTime());
		if(diff >= 0)
		{
			long diffDays = diff / (24 * 60 * 60 * 1000) + 1;
			double a = user.expenditures * 7.0 / diffDays;
			user.ltv = 52 * a * 20;
		}
	}
	
	//
	// Using heap to fetch top-k customers. The time complexity is O(N * log(k)), which can be viewed as linear since k 
	// is intuitively much smaller than the total number of customers N.
	// Note that we may further improve the efficiency by pre-building a max heap; however, frequent ltv update can make it expensive 
	// to maintain such a pre-built heap.
	//
	public ArrayList<Customer> TopXSimpleLTVCustomers(int k){
		
		// using min heap
	    Comparator<Customer> comparator = new Comparator<Customer>() {
	        @Override
	        public int compare(Customer o1, Customer o2) {
	            if(o1.ltv < o2.ltv) {
	                return -1;
	            } else if(o1.ltv > o2.ltv) {
	                return 1;
	            } else {
	                return 0;
	            }
	        }
	    };
		PriorityQueue<Customer> minHeap = new PriorityQueue<Customer>(comparator); 
		
		for(Customer user:customerProfile.values()) 
		{
			updateLTV(user, latestDate);
			minHeap.add(user);
			
			// remove those customers that are already determined to be not in top-K
			if(minHeap.size() > k) {
				minHeap.poll();
			}
		}
		
		return new ArrayList<> (minHeap);
	}
	
	
	public static void main(String[] args) {		
	
		Shutterfly sf = new Shutterfly();
		sf.Ingest("{\"type\": \"ORDER\", \"verb\": \"NEW\", \"key\": \"68d84e5d1a43\", \"event_time\": \"2017-01-06T12:55:55.555Z\", \"customer_id\": \"96f55c7d8f42\", \"total_amount\": \"12.34 USD\"}");
		sf.Ingest("{\"type\": \"CUSTOMER\", \"verb\": \"NEW\", \"key\": \"96f55c7d8f42\", \"event_time\": \"2017-01-06T12:46:46.384Z\", \"last_name\": \"Smith\", \"adr_city\": \"Middletown\", \"adr_state\": \"AK\"}");
		sf.Ingest("{\"type\": \"SITE_VISIT\", \"verb\": \"NEW\", \"key\": \"ac05e815502f\", \"event_time\": \"2017-01-06T12:45:52.041Z\", \"customer_id\": \"96f55c7d8f42\", \"tags\": [{\"some key\": \"some value\"}]}");
		sf.Ingest("{\"type\": \"IMAGE\", \"verb\": \"UPLOAD\", \"key\": \"d8ede43b1d9f\", \"event_time\": \"2017-01-06T12:47:12.344Z\", \"customer_id\": \"96f55c7d8f42\", \"camera_make\": \"Canon\", \"camera_model\": \"EOS 80D\"}");
		sf.Ingest("{\"type\": \"ORDER\", \"verb\": \"UPDATE\", \"key\": \"68d84e5d1a43\", \"event_time\": \"2017-01-07T12:55:55.555Z\", \"customer_id\": \"96f55c7d8f42\", \"total_amount\": \"15.34 USD\"}");
		sf.Ingest("{\"type\": \"ORDER\", \"verb\": \"UPDATE\", \"key\": \"68d84e5d1a43\", \"event_time\": \"2017-01-06T12:56:55.555Z\", \"customer_id\": \"96f55c7d8f42\", \"total_amount\": \"17.34 USD\"}");
		
		sf.Ingest("{\"type\": \"CUSTOMER\", \"verb\": \"NEW\", \"key\": \"96f55c7d8f43\", \"event_time\": \"2017-01-06T12:46:46.384Z\", \"last_name\": \"Smith2\", \"adr_city\": \"Middletown\", \"adr_state\": \"AK\"}");
		sf.Ingest("{\"type\": \"SITE_VISIT\", \"verb\": \"NEW\", \"key\": \"ac05e815202f\", \"event_time\": \"2017-01-06T12:45:52.041Z\", \"customer_id\": \"96f55c7d8f43\", \"tags\": [{\"some key\": \"some value\"}]}");
		sf.Ingest("{\"type\": \"ORDER\", \"verb\": \"UPDATE\", \"key\": \"68d84e5d1a49\", \"event_time\": \"2017-01-07T12:55:55.555Z\", \"customer_id\": \"96f55c7d8f43\", \"total_amount\": \"19.34 USD\"}");

		sf.Ingest("{\"type\": \"CUSTOMER\", \"verb\": \"NEW\", \"key\": \"96f55c7d8f44\", \"event_time\": \"2017-01-07T12:46:46.384Z\", \"last_name\": \"Smith3\", \"adr_city\": \"Middletown\", \"adr_state\": \"AK\"}");
		sf.Ingest("{\"type\": \"SITE_VISIT\", \"verb\": \"NEW\", \"key\": \"ac05e815512f\", \"event_time\": \"2017-01-07T12:45:52.041Z\", \"customer_id\": \"96f55c7d8f44\", \"tags\": [{\"some key\": \"some value\"}]}");
		sf.Ingest("{\"type\": \"ORDER\", \"verb\": \"UPDATE\", \"key\": \"68d84e5d1a47\", \"event_time\": \"2017-01-07T12:55:55.555Z\", \"customer_id\": \"96f55c7d8f44\", \"total_amount\": \"18.34 USD\"}");
		sf.Ingest("{\"type\": \"CUSTOMER\", \"verb\": \"UPDATE\", \"key\": \"96f55c7d8f44\", \"event_time\": \"2017-01-07T12:46:46.384Z\", \"last_name\": \"Smith4\", \"adr_city\": \"Middletown\", \"adr_state\": \"AK\"}");

		for(Customer user:sf.TopXSimpleLTVCustomers(1)) {
			System.out.println(user.customer_id + "\t" + user.last_name + "\t" + user.visits + "\t" + user.expenditures + "\t" + user.ltv);
		}
	}

}
