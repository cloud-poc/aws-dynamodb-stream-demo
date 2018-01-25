package org.akj.aws.test;

import static org.junit.Assert.assertNotNull;

import java.util.ArrayList;

import org.junit.Before;
import org.junit.Test;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.PutItemOutcome;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.BatchGetItemRequest;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.DescribeTableResult;
import com.amazonaws.services.dynamodbv2.model.GlobalSecondaryIndex;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.Projection;
import com.amazonaws.services.dynamodbv2.model.ProjectionType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.amazonaws.services.dynamodbv2.model.StreamSpecification;
import com.amazonaws.services.dynamodbv2.model.StreamViewType;
import com.amazonaws.services.dynamodbv2.model.TableStatus;

public class TableCreateTest {

	private DynamoDB dynamoDB = null;
	private AmazonDynamoDB client = null;
	private Table table = null;
	private final static String TABLE_NAME = "Infections";

	@Before
	public void setup() throws InterruptedException {
		client = AmazonDynamoDBClientBuilder.defaultClient();
		
		dynamoDB = new DynamoDB(Regions.AP_NORTHEAST_1);
		DescribeTableResult tableDesc = null;
		try {
			tableDesc = client.describeTable(TABLE_NAME);
		} catch (ResourceNotFoundException e) {
			System.out.printf("%s table does not exist \n", TABLE_NAME);
		}

		if (tableDesc == null || !tableDesc.getTable().getTableStatus().equals(TableStatus.ACTIVE.name())) {


			KeySchemaElement keySchema = new KeySchemaElement().withKeyType(KeyType.HASH)
					.withAttributeName("PatientId");
			/*
			 * keySchema.setKeyType(KeyType.HASH);
			 * keySchema.setAttributeName("PatientId");
			 */

			ArrayList<AttributeDefinition> attributeDefinitions = new ArrayList<AttributeDefinition>();
			attributeDefinitions.add(
					new AttributeDefinition().withAttributeType(ScalarAttributeType.S).withAttributeName("PatientId"));
			attributeDefinitions
					.add(new AttributeDefinition().withAttributeType(ScalarAttributeType.N).withAttributeName("Age"));
			attributeDefinitions
					.add(new AttributeDefinition().withAttributeType(ScalarAttributeType.S).withAttributeName("City"));

			ProvisionedThroughput provisionedThroughput = new ProvisionedThroughput(5L, 5L);
			GlobalSecondaryIndex globalSecondaryIndexes = new GlobalSecondaryIndex()
					.withIndexName("medical-status-in-city-by-age")
					.withKeySchema(new KeySchemaElement("Age", KeyType.HASH),
							new KeySchemaElement("City", KeyType.RANGE))
					.withProvisionedThroughput(new ProvisionedThroughput(5L, 5L))
					.withProjection(new Projection().withProjectionType(ProjectionType.ALL));

			StreamSpecification streamSpecification = new StreamSpecification().withStreamEnabled(true)
					.withStreamViewType(StreamViewType.NEW_AND_OLD_IMAGES);

			CreateTableRequest req = new CreateTableRequest().withKeySchema(keySchema)
					.withKeySchema(new KeySchemaElement("City", KeyType.RANGE))
					.withAttributeDefinitions(attributeDefinitions).withProvisionedThroughput(provisionedThroughput)
					.withGlobalSecondaryIndexes(globalSecondaryIndexes).withStreamSpecification(streamSpecification)
					.withTableName(TABLE_NAME);

			table = dynamoDB.createTable(req);

			table.waitForActive();
		} else {
			table = dynamoDB.getTable(TABLE_NAME);
		}
	}

	@Test
	public void testDataInsert() {
		assertNotNull(table);

		String json = "{\"date\":\"2017-12-10 15:30\",\"DoctorID\":\"xxx-0045\",\"Cat\": \"xxx\",\"Symptoms\": \"headache,cough\",\"Remark\":\"Headache is the symptom of pain anywhere in the region of the head or neck. It occurs in migraines (sharp, or throbbing pains), tension-type headaches, and cluster headaches.[1] Frequent headaches can affect relationships and employment.[1] There is also an increased risk of depression in those with severe headaches.[1]Headaches can occur as a result of many conditions whether serious or not. There are a number of different classification systems for headaches. The most well-recognized is that of the International Headache Society. Causes of headaches may include fatigue, sleep deprivation, stress, the effects of medications, the effects of recreational drugs, viral infections, loud noises, common colds, head injury, rapid ingestion of a very cold food or beverage, and dental or sinus issues\"}";
		Item item = new Item().withPrimaryKey("PatientId", "PX-XA-001").withInt("Age", 25)
				.withJSON("MedicalHistory", json).withString("Date", "2017-12-10 15:30").with("City", "Xi'an").withString("PatientName", "Alex KING");
		PutItemOutcome putItem = table.putItem(item);

		System.out.println(putItem.getPutItemResult());
	}
	
	@Test
	public void testGetDataList(){
//		BatchGetItemRequest batchGetItemRequest = new BatchGetItemRequest().with;
//		client.batchGetItem(batchGetItemRequest);
	}

}
