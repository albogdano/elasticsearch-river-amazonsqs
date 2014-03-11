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

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.sqs.AmazonSQSAsyncClient;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import java.io.IOException;
import java.util.ArrayList;
import org.elasticsearch.client.Client;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.river.AbstractRiverComponent;
import org.elasticsearch.river.River;
import org.elasticsearch.river.RiverName;
import org.elasticsearch.river.RiverSettings;
import java.util.List;
import java.util.Map;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;


/**
* @author Alex Bogdanovski <alex@erudika.com>
*/
public class AmazonsqsRiver extends AbstractRiverComponent implements River {

	private final Client client;
	private final AmazonSQSAsyncClient sqs;
	private final ObjectMapper mapper;
	private final String INDEX;
	private final String ACCESS_KEY;
	private final String SECRET_KEY;
	private final String QUEUE_URL;
	private final String REGION;
	private final String ENDPOINT;
	private final int MAX_MESSAGES;
	private final int SLEEP;
	private final int LONGPOLLING_INTERVAL;
	private final boolean DEBUG;
	private final int DEFAULT_MAX_MESSAGES = 10;
	private final int DEFAULT_LONGPOLLING_INTERVAL = 20;	// in seconds
	private final String DEFAULT_INDEX = "elasticsearch";
    private final int DEFAULT_SLEEP = 60;
    private final boolean DEFAULT_DEBUG = false;
	
	private volatile boolean closed = false;
	private volatile Thread thread;

	@SuppressWarnings({"unchecked"})
	@Inject
	public AmazonsqsRiver(RiverName riverName, RiverSettings settings, Client client) {
		super(riverName, settings);
		this.client = client;

		if (settings.settings().containsKey("amazonsqs")) {
			Map<String, Object> sqsSettings = (Map<String, Object>) settings.settings().get("amazonsqs");
			REGION = XContentMapValues.nodeStringValue(sqsSettings.get("region"), "null");
			ENDPOINT = XContentMapValues.nodeStringValue(sqsSettings.get("endpoint"), "null");
			ACCESS_KEY = XContentMapValues.nodeStringValue(sqsSettings.get("access_key"), "null");
			SECRET_KEY = XContentMapValues.nodeStringValue(sqsSettings.get("secret_key"), "null");
			QUEUE_URL = XContentMapValues.nodeStringValue(sqsSettings.get("queue_url"), "null");
			SLEEP = XContentMapValues.nodeIntegerValue(sqsSettings.get("sleep"), DEFAULT_SLEEP);
			LONGPOLLING_INTERVAL = XContentMapValues.nodeIntegerValue(sqsSettings.get("longpolling_interval"), DEFAULT_LONGPOLLING_INTERVAL);
			DEBUG = XContentMapValues.nodeBooleanValue(sqsSettings.get("debug"), DEFAULT_DEBUG);
		} else {
			REGION = settings.globalSettings().get("cloud.aws.region");
			ENDPOINT = settings.globalSettings().get("cloud.aws.endpoint");
			ACCESS_KEY = settings.globalSettings().get("cloud.aws.access_key");
			SECRET_KEY = settings.globalSettings().get("cloud.aws.secret_key");
			QUEUE_URL = settings.globalSettings().get("cloud.aws.sqs.queue_url");
			SLEEP = settings.globalSettings().getAsInt("cloud.aws.sqs.sleep", DEFAULT_SLEEP);
			LONGPOLLING_INTERVAL = settings.globalSettings().getAsInt("cloud.aws.sqs.longpolling_interval", DEFAULT_LONGPOLLING_INTERVAL);
			DEBUG = settings.globalSettings().getAsBoolean("cloud.aws.sqs.debug", DEFAULT_DEBUG);
		}

		if (settings.settings().containsKey("index")) {
			Map<String, Object> indexSettings = (Map<String, Object>) settings.settings().get("index");
			INDEX = XContentMapValues.nodeStringValue(indexSettings.get("index"), DEFAULT_INDEX);
			MAX_MESSAGES = XContentMapValues.nodeIntegerValue(indexSettings.get("max_messages"), DEFAULT_MAX_MESSAGES);
		} else {
			INDEX = DEFAULT_INDEX;
			MAX_MESSAGES = DEFAULT_MAX_MESSAGES;
		}
		
		if (ACCESS_KEY == null || ACCESS_KEY.trim().isEmpty() || ACCESS_KEY.equals("null")) {
			sqs = new AmazonSQSAsyncClient();
		} else {
			sqs = new AmazonSQSAsyncClient(new BasicAWSCredentials(ACCESS_KEY, SECRET_KEY));
		}
		
		String endpoint = ENDPOINT;
		if(ENDPOINT == null || ENDPOINT.trim().isEmpty() || ENDPOINT.equals("null")){
			endpoint = "https://sqs.".concat(REGION).concat(".amazonaws.com");
		}
		sqs.setEndpoint(endpoint);

		if (DEBUG) {
			if (ACCESS_KEY == null || ACCESS_KEY.length() < 2 || SECRET_KEY == null || SECRET_KEY.length() < 2) {
				logger.warn("AWS Credentials are not correct! "
					.concat("Access Key: ").concat(ACCESS_KEY + " ")
					.concat("Secret Key: ").concat(SECRET_KEY + " ")
					.concat("Endpoint: ").concat(endpoint));
				
			} else {
				logger.info("AWS Credentials: "
					.concat("Access Key length: ").concat(ACCESS_KEY.length() + " ")
					.concat("Secret Key length: ").concat(SECRET_KEY.length() + " ")
					.concat("Endpoint: ").concat(endpoint));
			}
		}
		mapper = new ObjectMapper();
	}

	public void start() {
		logger.info("creating amazonsqs river using queue {} (long polling interval: {}s)", QUEUE_URL, LONGPOLLING_INTERVAL);
		thread = EsExecutors.daemonThreadFactory(settings.globalSettings(), "amazonsqs_river").newThread(new Consumer());
		thread.start();
	}

	public void close() {
		if (closed) {
			return;
		}

		logger.info("closing amazonsqs river");
		closed = true;
		thread.interrupt();
	}

	private class Consumer implements Runnable {

		private int idleCount = 0;

		public void run() {
			String id = null;	// document id
			String type = null;	// document type
			String indexName = null; // document index
			Map<String, Object> data = null; // document data for indexing
			int interval = (LONGPOLLING_INTERVAL < 0 || LONGPOLLING_INTERVAL > 20) ? 
					DEFAULT_LONGPOLLING_INTERVAL : LONGPOLLING_INTERVAL;
			
			while (!closed) {
				// pull messages from SQS
				if (DEBUG) logger.info("Waiting {}s for messages...", interval);
				
				List<JsonNode> msgs = pullMessages(interval);

				try {
					BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();

					for (JsonNode msg : msgs) {
						if (msg.has("_id") && msg.has("_type")) {
							JsonNode idNode = msg.get("_id");
							id = idNode.isNumber() ? idNode.getNumberValue().toString() : idNode.getTextValue();
							type = msg.get("_type").getTextValue();
							//Support for dynamic indexes
							indexName = msg.has("_index") ? msg.get("_index").getTextValue() : INDEX;

							JsonNode dataNode = msg.get("_data");
							if (dataNode != null && dataNode.isObject()) {
								data = mapper.readValue(msg.get("_data"), new TypeReference<Map<String, Object>>() {});
								bulkRequestBuilder.add(client.prepareIndex(indexName, type, id).setSource(data).request());
							} else {
								bulkRequestBuilder.add(client.prepareDelete(indexName, type, id).request());
							}
						}
					}

					if (bulkRequestBuilder.numberOfActions() > 0) {						
						BulkResponse response = bulkRequestBuilder.execute().actionGet();
						if (response.hasFailures()) {
							logger.warn("Bulk operation completed with errors: "
									+ response.buildFailureMessage());
						}
						idleCount = 0;
					} else {
						idleCount++;
						if (DEBUG) logger.info("No new messages. {}", idleCount);
						// no tasks in queue => throttle down pull requests
						if (SLEEP > 0 && idleCount >= 3) {
							try {
								if (DEBUG) logger.info("Queue is empty. Sleeping for {}s", interval);
								Thread.sleep(SLEEP * 1000);
							} catch (InterruptedException e) {
								if (closed) {
									if (DEBUG) logger.info("Done.");
									break;
								}
							}
						}
					}
				} catch (Exception e) {
					logger.error("Bulk index operation failed {}", e);
					continue;
				}
			}
		}

		private List<JsonNode> pullMessages(int interval) {
			List<JsonNode> msgs = new ArrayList<JsonNode>();

			if (!isBlank(QUEUE_URL)) {
				try {
					ReceiveMessageRequest receiveReq = new ReceiveMessageRequest(QUEUE_URL);
					receiveReq.setMaxNumberOfMessages(MAX_MESSAGES);
					receiveReq.setWaitTimeSeconds(interval);
					List<Message> list = sqs.receiveMessage(receiveReq).getMessages();

					if (list != null && !list.isEmpty()) {
						if (DEBUG) logger.info("Received {} messages from queue.", list.size());
						for (Message message : list) {
							if (!isBlank(message.getBody())) {
								msgs.add(mapper.readTree(message.getBody()));
							}
							sqs.deleteMessage(new DeleteMessageRequest(QUEUE_URL, message.getReceiptHandle()));
						}
					}
				} catch (IOException ex) {
					logger.error(ex.getMessage());
				} catch (AmazonServiceException ase) {
					logException(ase);
				} catch (AmazonClientException ace) {
					logger.error("Could not reach SQS. {}", ace.getMessage());
				}
			}
			return msgs;
		}

		private void logException(AmazonServiceException ase) {
			logger.error("AmazonServiceException: error={}, statuscode={}, " + "awserrcode={}, errtype={}, reqid={}",
					ase.getMessage(), ase.getStatusCode(), ase.getErrorCode(), ase.getErrorType(), ase.getRequestId());
		}
	}

	private boolean isBlank(String str) {
		return str == null || str.isEmpty() || str.trim().isEmpty();
	}
}
