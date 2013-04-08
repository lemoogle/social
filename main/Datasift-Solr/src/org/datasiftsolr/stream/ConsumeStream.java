
package org.datasiftsolr.stream;

/**
 * This simple example demonstrates consuming a stream using the stream hash.
 */

import org.apache.solr.common.SolrDocument;
import org.datasift.EAccessDenied;
import org.datasift.ECompileFailed;
import org.datasift.EInvalidData;
import org.datasift.IStreamConsumerEvents;
import org.datasift.Interaction;
import org.datasift.JSONdn;
import org.datasift.StreamConsumer;
import org.datasift.User;
import org.datasift.dep.json.JSONArray;
import org.datasift.dep.json.JSONException;
import org.datasift.dep.json.JSONObject;

/**
 * @author MediaSift
 * @version 0.1
 */
public class ConsumeStream implements IStreamConsumerEvents {
	/**
	 * @param args
	 * @throws JSONException 
	 */
	
	public static void main2(String[] args) throws JSONException {
	Interaction a = new Interaction("{'a':{'b':'hello',c:'nested'}}");
	System.out.println(a);
	JSONArray b= new JSONArray();
	System.out.println(a.toJSONArray(b));
	System.out.println(b);
	}
	public static void main(String[] args) {
		try {


			// Authenticate
			System.out.println("Creating user...");
			User user = new User("martinzerbib", "dd9babe38bf574ef88ad3c1180a1b19b");

			// Create the consumer
			System.out.println("Getting the consumer...");
			StreamConsumer consumer = user.getConsumer(StreamConsumer.TYPE_HTTP, "a9d96c97ded1f1137bec53c59f20ce1a",
					new ConsumeStream());

			// And start consuming
			System.out.println("Consuming...");
			System.out.println("--");
			consumer.consume();
		} catch (EInvalidData e) {
			System.out.print("InvalidData: ");
			System.out.println(e.getMessage());
		} catch (ECompileFailed e) {
			System.out.print("CompileFailed: ");
			System.out.println(e.getMessage());
		} catch (EAccessDenied e) {
			System.out.print("AccessDenied: ");
			System.out.println(e.getMessage());
		}
	}

	/**
	 * Called when the connection has been established.
	 * 
	 * @param StreamConsumer consumer The consumer object.
	 */
	public void onConnect(StreamConsumer c) {
		System.out.println("Connected");
		System.out.println("--");
	}

	/**
	 * Called when the connection has disconnected.
	 * 
	 * @param StreamConsumer consumer The consumer object.
	 */
	public void onDisconnect(StreamConsumer c) {
		System.out.println("Disconnected");
		System.out.println("--");
	}

	public void doStuff(JSONObject i,SolrDocument doc,String s) throws JSONException{
		System.out.println(i);
		if(!s.equals("")){
			s+="_";
		}
		System.out.println(s);
		for (String name:JSONObject.getNames(i)){
		try{
			JSONObject o = i.getJSONObject(name);
			doStuff(o,doc,s+name);

		}
		catch(Exception e){
			doc.setField(s+name, i.getString(name));
		}
		
		}
		
		
	}
	
	/**
	 * Handle incoming data.
	 * 
	 * @param StreamConsumer
	 *            c The consumer object.
	 * @param Interaction
	 *            i The interaction data.
	 * @throws EInvalidData
	 */
	public void onInteraction(StreamConsumer c, Interaction i)
			throws EInvalidData {
		try {
			
			SolrDocument doc = new SolrDocument();
			doStuff(i, doc,"");
			System.out.println(doc);
			//System.out.println(i.getJSONArray("interaction"));
			//setFieldOrNot(doc,"author_username",i,"interaction.author.username");
			System.out.print(i.getStringVal("interaction.author.username"));
			System.out.print(": ");
			System.out.println(i);
		} catch (EInvalidData e) {
			// The interaction did not contain either a type or content.
			System.out.println("Exception: " + e.getMessage());
			System.out.print("Interaction: ");
			System.out.println(i);
		} catch (JSONException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println("--");
	}

	private void setFieldOrNot(SolrDocument doc, String field,Interaction i, String string) throws EInvalidData {

		if (i.has(string)){
			doc.setField(field, i.getStringVal(string));
		}
	}
	/**
	 * Handle delete notifications.
	 * 
	 * @param StreamConsumer
	 *            c The consumer object.
	 * @param Interaction
	 *            i The interaction data.
	 * @throws EInvalidData
	 */
	public void onDeleted(StreamConsumer c, Interaction i)
			throws EInvalidData {
		try {
			System.out.print("Deleted: ");
			System.out.print(i.getStringVal("interaction.id"));
		} catch (EInvalidData e) {
			// The interaction did not contain either a type or content.
			System.out.println("Exception: " + e.getMessage());
			System.out.print("Deletion: ");
			System.out.println(i);
		}
		System.out.println("--");
	}

	/**
	 * Handle status notifications
	 * 
	 * @param StreamConsumer
	 *            consumer The consumer object.
	 * @param String
	 *            type The status notification type.
	 * @param JSONdn
	 *            info The notification data.
	 */
	public void onStatus(StreamConsumer consumer, String type, JSONdn info) {
		System.out.print("STATUS: ");
		System.out.println(type);
	}

	/**
	 * Called when the consumer has stopped.
	 * 
	 * @param DataSift_StreamConsumer
	 *            consumer The consumer object.
	 * @param string
	 *            reason The reason the consumer stopped.
	 */
	public void onStopped(StreamConsumer consumer, String reason) {
		System.out.print("Stopped: ");
		System.out.println(reason);
	}

	/**
	 * Called when a warning is received in the data stream.
	 * 
	 * @param DataSift_StreamConsumer consumer The consumer object.
	 * @param string message The warning message.
	 */
	public void onWarning(StreamConsumer consumer, String message)
			throws EInvalidData {
		System.out.println("Warning: " + message);
	}

	/**
	 * Called when an error is received in the data stream.
	 * 
	 * @param DataSift_StreamConsumer consumer The consumer object.
	 * @param string message The error message.
	 */
	public void onError(StreamConsumer consumer, String message)
			throws EInvalidData {
		System.out.println("Error: " + message);
	}
}