/*
 * Copyright 2013 Alex Bogdanovski <alex@erudika.com>.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * You can reach the author at: https://github.com/albogdano
 */
package org.elasticsearch.river.amazonsqs;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.CreateQueueResult;
import com.amazonaws.services.sqs.model.DeleteQueueRequest;
import com.amazonaws.services.sqs.model.SendMessageBatchRequest;
import com.amazonaws.services.sqs.model.SendMessageBatchRequestEntry;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.elasticmq.rest.sqs.SQSRestServer;
import org.elasticmq.rest.sqs.SQSRestServerBuilder;
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
 * @author Alex Bogdanovski <alex@erudika.com>
 */
public class AmazonsqsRiverTest {
	
	private static AmazonSQSClient sqs;
	private static Client client;
	private final static String messageTemplate = "{ \"_id\": \"#\", \"_index\": \"testindex1\", "
			+ "\"_type\": \"testtype1\", \"_data\": { \"key#\": \"value#\" } }";
	private int msgId = 1;
	private static SQSRestServer sqsServer;
	private static String queueURL;
	private static String endpoint = "http://localhost:9324";
//	private static String endpoint = "https://sqs.eu-west-1.amazonaws.com";
	
	public AmazonsqsRiverTest() {
	}
	
	@BeforeClass
	public static void setUpClass() throws Exception{
		sqsServer = SQSRestServerBuilder.start();
		sqs = new AmazonSQSClient(new BasicAWSCredentials(System.getProperty("accesskey", "x"), 
				System.getProperty("secretkey", "x")));
		sqs.setEndpoint(endpoint);
		CreateQueueResult q = sqs.createQueue(new CreateQueueRequest().withQueueName("testq"));
		queueURL = q.getQueueUrl();
		
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
							field("endpoint", endpoint).
							field("access_key", System.getProperty("accesskey", "x")).
							field("secret_key", System.getProperty("secretkey", "x")).
							field("queue_url", System.getProperty("queueurl", queueURL)).
							field("debug", "true").					
						endObject().
					endObject()).execute().actionGet();
	}

	private static void stopElasticSearchInstance() {
		System.out.println("shutting down elasticsearch");
//		client.admin().cluster().prepareNodesShutdown().execute().actionGet();
		client.close();
	}

	@AfterClass
	public static void tearDownClass() throws Exception{
		sqs.deleteQueue(new DeleteQueueRequest(queueURL));
		sqsServer.stopAndWait();
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
		
		CountResponse count = client.prepareCount("testindex1").setQuery(QueryBuilders.matchAllQuery()).get();
		long c = count.getCount();
		Assert.assertEquals(11L, c);
	}

	
	private void postMessageToQueue(String msgText) {
        try {
			sqs.sendMessage(new SendMessageRequest(System.getProperty("queueurl", queueURL), msgText));
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
					sqs.sendMessageBatch(new SendMessageBatchRequest(System.getProperty("queueurl", queueURL)).withEntries(entries));
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