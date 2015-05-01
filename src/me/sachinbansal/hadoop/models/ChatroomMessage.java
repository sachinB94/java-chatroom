package me.sachinbansal.hadoop.models;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Date;

import org.apache.pig.ExecType;
import org.apache.pig.PigServer;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;

import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoException;
import com.mongodb.ServerAddress;
import com.mongodb.WriteConcern;

import me.sachinbansal.hadoop.analysis.Analysis;

public class ChatroomMessage {

	/**
	 * The IP address the message came from.
	 */
	private String fromAddress = null;

	/**
	 * The message itself.
	 */
	private String message = null;
	
	/**
	 * Time of message
	 */
	private Date time = null;

	/**
	 * Maximum number of messages in DB
	 */
	private long MAXMessageCount = 10;


	private static MongoClient mongoClient;
	private static DB db;

	// Collection to be emptied
	private static DBCollection messageCollection;
	// Collection to save data to hadoop
	private static DBCollection messageCollectionBackup;

	/**
	 * Creating connection to MongoDB
	 */
	public ChatroomMessage() {

	}

	public ChatroomMessage(String fromAddress, String message) {
		this.fromAddress = fromAddress;
		this.message = message;
	}

	public void createMongoConnection() throws UnknownHostException, MongoException{
		MongoClientOptions.Builder builder = new MongoClientOptions.Builder();
		builder.writeConcern(WriteConcern.JOURNAL_SAFE);
		mongoClient = new MongoClient(
				new ServerAddress("localhost"), builder.build());
		db = mongoClient.getDB("chatroom");
		messageCollection = db.getCollection("message");
		messageCollectionBackup = db.getCollection("messageBackup");
	}
	
	public void save() throws IOException, InterruptedException {
		this.saveToMongo();
		ChatroomMessage crm = this;
		if (this.getMessageCount() >= this.MAXMessageCount) {
			System.setProperty("HADOOP_HOME", "/usr/local/hadoop");
			System.setProperty("HADOOP_USER_NAME", "hadoop");
			
			System.out.println("time to hadoopify");
			this.takeBackup();
			Thread deleteOperation = new Thread(new Runnable() {
				@Override
				public void run() {
					deleteMessageCollection();
				}
			});
			
			Thread saveOperation = new Thread(new Runnable() {
				@Override
				public void run() {
					try {
						crm.saveToHadoop();
						new Analysis().countMessage(crm.time);
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (Exception e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			});
			
			deleteOperation.start();
			saveOperation.start();
			
			deleteOperation.join();
		}
	}

	public void saveToMongo() {
		messageCollection.insert(this.getDBObject());
	}

	public long getMessageCount() {
		return messageCollection.getCount();
	}
	
	public void takeBackup() {
		messageCollection.aggregate(Arrays.asList((DBObject)new BasicDBObject("$out", "messageBackup")));
	}
	
	public static void deleteMessageCollection() {
		messageCollection.remove(new BasicDBObject());
	}
	
	public static void deleteBackupCollection() {
		messageCollectionBackup.remove(new BasicDBObject());
	}
	
	public void saveToHadoop() throws IOException {
		PigServer pigServer = new PigServer(ExecType.MAPREDUCE);
		String pigQuery = "REGISTER 'WebContent/WEB-INF/lib/mongo-hadoop-core-1.3.2.jar';"
				+ "REGISTER 'WebContent/WEB-INF/lib/mongo-hadoop-pig-1.3.0.jar';"
				+ " A = LOAD 'mongodb://localhost:27017/chatroom.messageBackup'"
				+ " USING com.mongodb.hadoop.pig.MongoLoader('address, message, time')"
				+ " AS (address:chararray, message:chararray, time:datetime);";
		pigServer.registerQuery(pigQuery);
		pigServer.store("A", "/user/luffy/chatroom/" + this.time.toString().replaceAll(" ", "_").replaceAll(":", "-"));
		pigServer.shutdown();
		deleteBackupCollection();
	}
	
	/**
	 * Returns a formatted version of the message.
	 *
	 * @return
	 */
	public String print() {
		return "<p>[" + this.fromAddress + "] " + this.message + "</p>";
	}

	public BasicDBObject getDBObject() {
		this.time = new Date();
		BasicDBObject doc = new BasicDBObject("address", this.fromAddress)
		.append("message", this.message)
		.append("time", this.time);
		return doc;
	}
}