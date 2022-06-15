package com.runwu.lambda;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.DynamodbEvent;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * lambda from Dynamodb to S3
 * @author Pan Yan
 * @date 2022-05-30
 */
public class Handler implements RequestHandler<DynamodbEvent, String> {

    private static final String AMAZON_AWS_ACCESS_KEY = "AKIARH7ZCAJQQG5JQVLN";
    private static final String AMAZON_AWS_SECREY_KEY = "5y8nPh5zas7X0/d/tmpICIfhwPlojcK/J+zmDzma";
    private static final String AMAZON_AWS_REGION = "us-east-1";

    private static final String DST_BUCKET = "runworld/transaction";

    @Override
    public String handleRequest(DynamodbEvent dynamodbEvent, Context context) {

        List<DynamodbEvent.DynamodbStreamRecord> records = dynamodbEvent.getRecords();

        for (DynamodbEvent.DynamodbStreamRecord record:records) {
            JSONObject jsonObject = new JSONObject();
            java.util.Map<String, AttributeValue> map =  record.getDynamodb().getNewImage();
            Set<Map.Entry<String, AttributeValue>> set =  map.entrySet();
            // Parsing the data of the first layer
            for (Map.Entry<String, AttributeValue> entry:set) {
                String key = entry.getKey();
                AttributeValue attributeValue = entry.getValue();
                //Trying to get string type
                Object value = attributeValue.getS();
                if (value==null) {
                    //Trying to get number type
                    value = attributeValue.getN();
                }
                if (value==null) {
                    //Trying to get map type
                    Map<String, AttributeValue> mapVal2 = attributeValue.getM();
                    value = parseAvlM(mapVal2);
                }
                if (value==null) {
                    //Trying to get list type
                    List<AttributeValue> valueList = attributeValue.getL();
                    value = parseAvlLM(valueList);
                }
                jsonObject.put(key, value);
            }





            System.out.println(record);
            System.out.println("transaction json is "+jsonObject.toJSONString());
            AWSCredentialsProvider provider = new AWSStaticCredentialsProvider(new BasicAWSCredentials(AMAZON_AWS_ACCESS_KEY, AMAZON_AWS_SECREY_KEY));
            AmazonS3 s3Client = AmazonS3ClientBuilder.standard().withCredentials(provider).withRegion(Regions.fromName(AMAZON_AWS_REGION)).build();
            s3Client.putObject(DST_BUCKET, "2022022711300001", "test content");
        }
        return "Ok";
    }







    private JSONArray parseAvlLM(List<AttributeValue> valueList) {
        JSONArray array = new JSONArray();
        if (valueList==null) {
            return array;
        }
        //Parsing the data of the second layer
        for (AttributeValue avl2:valueList) {
            java.util.Map<String, AttributeValue> mapVal2 = avl2.getM();
            JSONObject jsonObject2 = parseAvlM(mapVal2);
            array.add(jsonObject2);
        }
        return array;
    }

    private JSONObject parseAvlM(Map<String, AttributeValue> mapVal) {

        if (mapVal!=null) {
            JSONObject jsonObject = new JSONObject();
            Set<Map.Entry<String, AttributeValue>> setVal =  mapVal.entrySet();
            for (Map.Entry<String, AttributeValue> entry:setVal) {
                String key = entry.getKey();
                AttributeValue attributeValue = entry.getValue();
                Object value = attributeValue.getS();
                if (value==null) {
                    value = attributeValue.getN();
                }
                jsonObject.put(key, value);
            }
            return jsonObject;
        }
        return null;
    }

}
