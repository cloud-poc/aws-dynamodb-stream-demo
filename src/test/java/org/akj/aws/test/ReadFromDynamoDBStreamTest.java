package org.akj.aws.test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBStreams;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBStreamsClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.DeleteRequest;
import com.amazonaws.services.dynamodbv2.model.DescribeStreamRequest;
import com.amazonaws.services.dynamodbv2.model.DescribeStreamResult;
import com.amazonaws.services.dynamodbv2.model.DescribeTableResult;
import com.amazonaws.services.dynamodbv2.model.GetRecordsRequest;
import com.amazonaws.services.dynamodbv2.model.GetRecordsResult;
import com.amazonaws.services.dynamodbv2.model.GetShardIteratorRequest;
import com.amazonaws.services.dynamodbv2.model.GetShardIteratorResult;
import com.amazonaws.services.dynamodbv2.model.PutRequest;
import com.amazonaws.services.dynamodbv2.model.Record;
import com.amazonaws.services.dynamodbv2.model.Shard;
import com.amazonaws.services.dynamodbv2.model.ShardIteratorType;
import com.amazonaws.services.dynamodbv2.model.StreamSpecification;
import com.amazonaws.services.dynamodbv2.model.WriteRequest;

public class ReadFromDynamoDBStreamTest {

	private AmazonDynamoDBStreams dynamoDBStreamClient = null;
	private final static String TABLE_NAME = "Infections";
	AmazonDynamoDB dynamoDBClient = null;
	DynamoDB dynamoDB = null;
	Table table = null;
	String streamArn = null;

	@Before
	public void setup() {
		dynamoDBStreamClient = AmazonDynamoDBStreamsClientBuilder.defaultClient();
		dynamoDBClient = AmazonDynamoDBClientBuilder.standard().build();
		dynamoDB = new DynamoDB(dynamoDBClient);
		table = dynamoDB.getTable(TABLE_NAME);

		// Print the stream settings for the table
		DescribeTableResult describeTableResult = dynamoDBClient.describeTable(TABLE_NAME);
		streamArn = describeTableResult.getTable().getLatestStreamArn();
		System.out.println(
				"Current stream ARN for " + TABLE_NAME + ": " + describeTableResult.getTable().getLatestStreamArn());
		StreamSpecification streamSpec = describeTableResult.getTable().getStreamSpecification();
		System.out.println("Stream enabled: " + streamSpec.getStreamEnabled());
		System.out.println("Update view type: " + streamSpec.getStreamViewType());

	}

	@Test
	public void testReadFromStream() {
		String lastEvaluatedShardId = null;

		do {
			DescribeStreamResult describeStreamResult = dynamoDBStreamClient.describeStream(new DescribeStreamRequest()
					.withStreamArn(streamArn).withExclusiveStartShardId(lastEvaluatedShardId));
			List<Shard> shards = describeStreamResult.getStreamDescription().getShards();
			System.out.println("Shard count:" + shards.size());

			for (Shard shd : shards) {
				GetShardIteratorRequest getShardIteratorRequest = new GetShardIteratorRequest().withStreamArn(streamArn)
						.withShardId(shd.getShardId()).withShardIteratorType(ShardIteratorType.TRIM_HORIZON);

				GetShardIteratorResult getShardIteratorResult = dynamoDBStreamClient
						.getShardIterator(getShardIteratorRequest);
				String currentShardIter = getShardIteratorResult.getShardIterator();

				while (currentShardIter != null) {
					// System.out.println(" Shard iterator: " +
					// currentShardIter.substring(380));
					GetRecordsRequest getRecordsRequest = new GetRecordsRequest().withShardIterator(currentShardIter);
					GetRecordsResult records = dynamoDBStreamClient.getRecords(getRecordsRequest);
					for (Record rcd : records.getRecords()) {
						System.out.printf(
								"EventID is: %s, Event Name is: %s, Event source is: %s, Event Version is: %s \n",
								rcd.getEventID(), rcd.getEventName(), rcd.getEventSource(), rcd.getEventVersion());

						System.out.println(rcd.getDynamodb());
					}

					currentShardIter = records.getNextShardIterator();
				}

			}

			lastEvaluatedShardId = describeStreamResult.getStreamDescription().getLastEvaluatedShardId();

		} while (lastEvaluatedShardId != null);

	}

	@Test
	public void testBatchUpdateAndDelete() {
		int count = 50;
		int counters = 0;
		java.util.Map<String, java.util.List<WriteRequest>> requestItems = new HashMap<String, List<WriteRequest>>();
		List<WriteRequest> list = new java.util.ArrayList<WriteRequest>();
		List<WriteRequest> delList = new java.util.ArrayList<WriteRequest>();

		for (int i = 1; i <= count; i++) {
			Map<String, AttributeValue> items = new HashMap<String, AttributeValue>();
			items.put("PatientId", new AttributeValue("xxx-CN-XA-" + i));
			items.put("City", new AttributeValue("XI'AN"));
			items.put("PatientName", new AttributeValue("ROBOT-TEST-" + i));
			items.put("Age", new AttributeValue().withN(20 + i + ""));

			// batch insert data into db
			PutRequest putRequest = new PutRequest().withItem(items);
			WriteRequest withPutRequest = new WriteRequest().withPutRequest(putRequest);
			list.add(withPutRequest);

			if (i % 25 == 0) {
				requestItems.put(TABLE_NAME, list);

				dynamoDBClient.batchWriteItem(requestItems);

				requestItems.clear();
				list.clear();
			}

			// batch delete data just inserted
			Map<String, AttributeValue> delItems = new HashMap<String, AttributeValue>();
			delItems.put("PatientId", new AttributeValue("xxx-CN-XA-" + i));
			delItems.put("City", new AttributeValue("XI'AN"));
			/* delItems.put("Age",new AttributeValue().withN(20 + i + "")); */
			DeleteRequest deleteRequest = new DeleteRequest(delItems);
			WriteRequest withDeleteRequest = new WriteRequest().withDeleteRequest(deleteRequest);
			delList.add(withDeleteRequest);
			if (i % 25 == 0) {
				requestItems.put(TABLE_NAME, delList);

				dynamoDBClient.batchWriteItem(requestItems);

				requestItems.clear();
				delList.clear();
			}

		}

	}

}
