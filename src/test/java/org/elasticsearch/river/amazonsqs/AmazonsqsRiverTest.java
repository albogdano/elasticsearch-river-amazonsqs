/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.elasticsearch.river.amazonsqs;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.SendMessageBatchRequest;
import com.amazonaws.services.sqs.model.SendMessageBatchRequestEntry;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.elasticsearch.action.count.CountRequest;
import org.elasticsearch.action.count.CountResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.ImmutableSettings;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.indices.IndexMissingException;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 *
 * @author Alex Bogdanovski <albogdano@me.com>
 */
public class AmazonsqsRiverTest {
	
	private static AmazonSQSClient sqs;
	private static Client client;
	private final static String messageTemplate = "{ \"_id\": \"#\", \"_index\": \"testindex1\", \"_type\": \"testtype1\", \"_data\": { \"key#\": \"value#\" } }";
	private int msgId = 1;
	
	public AmazonsqsRiverTest() {
	}
	
	@BeforeClass
	public static void setUpClass() throws Exception{
		String endpoint = "https://sqs.eu-west-1.amazonaws.com";
		if (System.getProperty("accesskey") == null || System.getProperty("secretkey") == null) {
			Assert.fail("AWS credentials missing.");
		}
		if (System.getProperty("queueurl") == null) {
			Assert.fail("AWS queue url missing.");
		}
		sqs = new AmazonSQSClient(new BasicAWSCredentials(System.getProperty("accesskey"), System.getProperty("secretkey")));
		sqs.setEndpoint(endpoint);
		startElasticSearchDefaultInstance();
	}
	
	private static void startElasticSearchDefaultInstance() throws IOException {
		Node node = NodeBuilder.nodeBuilder().settings(ImmutableSettings.settingsBuilder().put("gateway.type", "none")).node();
		client = node.client();
		client.prepareIndex("_river", "test1", "_meta").
				setSource(jsonBuilder().
					startObject().
						field("type", "amazonsqs").				
						startObject("amazonsqs").
							field("region", "eu-west-1").
							field("access_key", System.getProperty("accesskey", "none")).
							field("secret_key", System.getProperty("secretkey", "none")).
							field("queue_url", System.getProperty("queueurl", "none")).
							field("debug", "true").					
						endObject().
					endObject()).execute().actionGet();
	}

	private static void stopElasticSearchInstance() {
		System.out.println("shutting down elasticsearch");
		client.admin().cluster().prepareNodesShutdown().execute();
		client.close();
	}

	@AfterClass
	public static void tearDownClass() throws Exception{
		stopElasticSearchInstance();
	}
	
	@Before
	public void setUp() {
	}
	
	@After
	public void tearDown() {
	}

	/**
	 * Test of start method, of class AmazonsqsRiver.
	 */
	@Test
	public void testStart() throws Exception{
		// assure that the index is not yet there
		try {
			client.get(new GetRequest("testindex1", "testtype1", "1")).actionGet();
			Assert.fail();
		} catch (IndexMissingException idxExcp) { }

		postMessageToQueue(generateMessage(1));

		Thread.sleep(3000l);
		
		GetResponse resp = client.get(new GetRequest("testindex1", "testtype1", "1")).actionGet();
		Assert.assertEquals("{\"key1\":\"value1\"}", resp.getSourceAsString());
		
		Thread.sleep(3 * 1000);
		
		postMessagesToQueue(10);
		
		Thread.sleep(3000);
		
		CountResponse count = client.count(new CountRequest("testindex1").query(QueryBuilders.matchAllQuery())).actionGet();
		long c = count.getCount();
		Assert.assertEquals(11L, c);
	}

	
	private void postMessageToQueue(String msgText) {
        try {
			sqs.sendMessage(new SendMessageRequest(System.getProperty("queueurl"), msgText));
		} catch (Exception e) {
			Assert.fail("Failed to send the message."+e);
		}
	}
	
	private void postMessagesToQueue(int count) {
        try {
			List<SendMessageBatchRequestEntry> entries = new ArrayList<SendMessageBatchRequestEntry>();
			int j = 0;
			for (int i = 0; i < count; i++) {
				entries.add(new SendMessageBatchRequestEntry(Integer.toString(i), generateMessage(null)));
				if(++j > 9){
					sqs.sendMessageBatch(new SendMessageBatchRequest(System.getProperty("queueurl")).withEntries(entries));
					entries.clear();
					Thread.sleep(100);
				}
			}
		} catch (Exception e) {
			Assert.fail("Failed to send the message."+e);
		}
	}
	
	private String generateMessage(Integer i){
		return messageTemplate.replaceAll("#", (i != null) ? i.toString() : Integer.toString(++msgId));
	}
}