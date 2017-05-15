package com.amazonsamples;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.SdkClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.regions.AwsProfileRegionProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.CreateBucketRequest;
import com.amazonaws.services.s3.model.DeleteBucketRequest;
import com.amazonaws.services.s3.model.DeleteObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3ObjectSummary;

public class S3App {

	public static void main(String[] args) throws IOException {
		System.out.println("Hello World AWS");
		List<Bucket> buckets = listBuckets();
		//createNewBucket();
		//buckets = listBuckets();
		//putObject(buckets.get(4).getName());
		//listObjects(buckets.get(4).getName());
		//deleteObjects(buckets.get(4).getName());
		//deleteBucket(buckets.get(4).getName());
		
	}

	private static void deleteObjects(String name) {
		List<S3ObjectSummary> objects = listObjects(name);
		AmazonS3 s3Client = S3ClientFactory.getInstance();
		
		for(S3ObjectSummary obj :objects)
		{
			DeleteObjectRequest deleteReq = new DeleteObjectRequest(name, obj.getKey());
			s3Client.deleteObject(deleteReq);
		}
	}

	private static void deleteBucket(String name) {
		AmazonS3 s3Client = S3ClientFactory.getInstance();
		DeleteBucketRequest deleteReq = new DeleteBucketRequest(name);
		s3Client.deleteBucket(deleteReq);
		
	}

	
	private static void putObject(String name) {
		File fileToPut = new File("C:/Users/Public/Pictures/Sample Pictures/Desert.jpg");
		String key ="desert-img";
		PutObjectRequest putReq = new PutObjectRequest(name,key, fileToPut);
		AmazonS3 s3Client = S3ClientFactory.getInstance();
		s3Client.putObject(putReq);
	}

	private static List<S3ObjectSummary> listObjects(String bucketName){
		
		ListObjectsV2Request listReq = new ListObjectsV2Request();
		listReq.setBucketName(bucketName);
		listReq.setDelimiter("/");
		AmazonS3 s3Client = S3ClientFactory.getInstance();
		ListObjectsV2Result result = s3Client.listObjectsV2(listReq);
		
		for(S3ObjectSummary objSummary : result.getObjectSummaries()){
			System.out.println(objSummary.getKey() + "-"+ objSummary.getLastModified());
		}
		
		return result.getObjectSummaries();
	}
	
	private static void createNewBucket() {
		AmazonS3 s3Client = S3ClientFactory.getInstance();
		AwsProfileRegionProvider regionProvider = new AwsProfileRegionProvider("default");
		CreateBucketRequest createReq =  new CreateBucketRequest("spbucket2017", regionProvider.getRegion());
		s3Client.createBucket(createReq);
	}

	private static List<Bucket> listBuckets() {
		List<Bucket> buckets =null;
		try {
			AmazonS3 amazonS3 = createS3Client();

			buckets = amazonS3.listBuckets();
			for (Bucket b : buckets) {
				System.out.println(b.getName() + " - " + b.getCreationDate());
			}
		} catch (AmazonServiceException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SdkClientException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return buckets;
	}

	private static AmazonS3 createS3Client() {
		ClientConfiguration clientConf = new ClientConfiguration();
		clientConf.setProxyHost("webproxy.twc.state.tx.us");
		clientConf.setProxyPort(9092);
		clientConf.setConnectionTimeout(60 * 1000);
		AWSCredentials credentials;
		try {
			credentials = getAwsCredentials();

			AWSStaticCredentialsProvider credentialsProvider = new AWSStaticCredentialsProvider(credentials);
			AwsProfileRegionProvider regionProvider = new AwsProfileRegionProvider("default");
			AmazonS3 amazonS3 = AmazonS3ClientBuilder.standard().withClientConfiguration(clientConf)
					.withCredentials(credentialsProvider).withRegion(regionProvider.getRegion()).build();
			return amazonS3;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}

	private static AWSCredentials getAwsCredentials() throws IOException {
		AWSCredentials awsCredentials;
		try (InputStream ins = S3App.class.getResourceAsStream("/credentials.properties")) {
			if (ins == null)
				throw new RuntimeException("credentials not setup");
			awsCredentials = new PropertiesCredentials(ins);
		}
		return awsCredentials;
	}

	static class S3ClientFactory {
		private static AmazonS3 client = S3App.createS3Client();

		private S3ClientFactory() {
		};

		public static AmazonS3 getInstance() {
			if (client == null)
				client = S3App.createS3Client();
			return client;
		}

	}

}
