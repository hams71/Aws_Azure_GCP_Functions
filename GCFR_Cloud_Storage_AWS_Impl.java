package com.teradata.gcfr.common.cloudImpl;


import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.channels.Channels;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.Scanner;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Pattern;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;
import org.slf4j.LoggerFactory;




//AWS SDK for JAVA 2.x
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.ListObjectsRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsResponse;
import software.amazon.awssdk.services.s3.model.NoSuchBucketException;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.services.s3.model.CopyObjectRequest;
import software.amazon.awssdk.services.s3.model.CopyObjectResponse;
import software.amazon.awssdk.services.s3.model.CopyObjectResult;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectResponse;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadBucketRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CopyObjectRequest;
import software.amazon.awssdk.services.s3.model.CopyObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.RequestBody;


public class GCFR_Cloud_Storage_AWS_Impl implements GCFR_Cloud_Storage_Int {
	
	org.slf4j.Logger logger = LoggerFactory.getLogger(GCFR_Cloud_Storage_AWS_Impl.class);
	responseDTO response = null;
	private Logger processLogger;
	private CustomUtils utils;
	

	private String CLOUD_PROFILE; //default
	private String LANDING_DIRECTORY; //"s3://gcfrtestbuck"
	
	private S3Client s3;
	private String bucketName;
	LinkedHashMap<String, String> objectNames;
	private String tempStartStr = "s3://";
	
	public List<String> failedCopyFiles;
	
	
	public GCFR_Cloud_Storage_AWS_Impl(String CP, String LD, requestDTO request){
		this.CLOUD_PROFILE = CP;
		this.LANDING_DIRECTORY = LD;
		
		utils = new CustomUtils();
		failedCopyFiles = new ArrayList<>();
		initProccesLoggerInstance(request);
	}
	
	
	@Override
	public Integer aunthenticateIntoCloud() {
		
		//Use credentials through profile name
        this.s3 = S3Client.builder().credentialsProvider(ProfileCredentialsProvider.create(CLOUD_PROFILE)).build();
        
        this.bucketName = returnBucketName(LANDING_DIRECTORY);
            
        if(checkBucketExists(this.bucketName) == false) {
        	return 13;
        }
        
        logMsg("AWS S3 Bucket "+ this.bucketName+ " exists.");

        
        //s3 Object Names are also called Keys
        //Linked Hash Map used to preserve ordering of elements
        //Key is name, Value is type of S3 object e.g Control file or Data file
        this.objectNames = new LinkedHashMap<>();

		return 0;
	}
	
	//to split bucket url into only bucket name (for AWS)
    //s3://gcfrtestbuck/loading -> gcfrtestbuck/loading
	@Override
	public String splitBucketUrl(String bucketUrl, String tempStartStr) {
    	String returnBucketName = "";
    	 
    	if (bucketUrl.startsWith(tempStartStr)) {
    		if(bucketUrl.endsWith("/")) {
          		//remove the "s3://" and remove the last slash "/" 
          		//returnBucketName = bucketUrl.split(tempStartStr)[1].split("/")[0]; //doesnt work if "s3://buck/load/"
    			returnBucketName = bucketUrl.split(tempStartStr)[1];
    			returnBucketName = removeLastChar(returnBucketName);
          		return returnBucketName;
          	}
          	else { // no / suffix attached
          	    returnBucketName = bucketUrl.split(tempStartStr)[1];
          	    return returnBucketName;
          	}
        }		
        else {  //no prefix attached
        	if(bucketUrl.endsWith("/")) {
        		returnBucketName = removeLastChar(bucketUrl);
          		return returnBucketName;
        	}
        	else {
        		return bucketUrl;
        	}	
        }
    }
    
    //remove last char e.g "/"
    private String removeLastChar(String s)   
    {  
    	//returns the string after removing the last character  
    	return s.substring(0, s.length() - 1);  
    }

	 ///////////////////////////
    //Gets/Lists All S3 Bucket Objects
    ///////////////////////////
	@Override
	public LinkedHashMap<String, String> listBucketObjects() {
		 try {
             ListObjectsRequest listObjects = ListObjectsRequest
                     .builder()
                     .bucket(this.bucketName)
                     .build();
             ListObjectsResponse res = s3.listObjects(listObjects);
             
             List<S3Object> objects = res.contents();               

             for (ListIterator iterVals = objects.listIterator(); iterVals.hasNext(); ) {
                 S3Object myValue = (S3Object) iterVals.next();
                 //logMsg("TBD..The name of the object is " + myValue.key());
                
                 //Save file name and its extension
                  this.objectNames.put(myValue.key(), getExtensionByApacheCommonLib(myValue.key()));
              }
         } 
		 
		 catch (S3Exception e) {
			 logErrMsg("AWS ERROR:"+e.awsErrorDetails().errorMessage());
         }
        
         return objectNames;        
	}
	
	public S3Object listcustomBucketObjects(String bucketName,String Prefix) {
		 try {
             ListObjectsRequest listObjects = ListObjectsRequest
                     .builder()
                     .prefix(Prefix)
                     .bucket(bucketName)
                     .build();
             ListObjectsResponse res = s3.listObjects(listObjects);
             
             List<S3Object> objects = res.contents();               

             for (ListIterator iterVals = objects.listIterator(); iterVals.hasNext(); ) {
                 S3Object myValue = (S3Object) iterVals.next();
                 //logMsg("TBD..The name of the object is " + myValue.key());
                 return myValue;
              }
         } 
		 
		 catch (S3Exception e) {
			 logErrMsg("AWS ERROR:"+e.awsErrorDetails().errorMessage());
         }
		 return null;
	}
	
	public LinkedHashMap<String, String> listBucketObjects(String bucketName){
		
		 try {
             ListObjectsRequest listObjects = ListObjectsRequest
                     .builder()
                     .bucket(bucketName)
                     .build();
             ListObjectsResponse res = s3.listObjects(listObjects);
             
             List<S3Object> objects = res.contents();               

             for (ListIterator iterVals = objects.listIterator(); iterVals.hasNext(); ) {
                 S3Object myValue = (S3Object) iterVals.next();
                 //logMsg("TBD..The name of the object is " + myValue.key());
                
                 //Save file name and its extension
                  this.objectNames.put(myValue.key(), getExtensionByApacheCommonLib(myValue.key()));
              }
         } 
		 
		 catch (S3Exception e) {
			 logErrMsg("AWS ERROR:"+e.awsErrorDetails().errorMessage());
         }
        
         return objectNames;        
	}
	

	@Override
	public boolean checkBucketExists(String bucketName) {
		HeadBucketRequest headBucketRequest = HeadBucketRequest.builder()
	            .bucket(bucketName)
	            .build();

	    try {
	        s3.headBucket(headBucketRequest);
	        return true;
	    }  
	    catch (Exception e) {
	    	logErrMsg(" aws bucket does not exist or storage access issue.."+e.getLocalizedMessage());
	        return false;
	    }


	}

	@Override
	public boolean checkBucketFileExists(String fileName, String bucketPrefix ,Integer type) {
		try {	
				String fileNameWithPrefix = constructFileName(bucketPrefix, fileName);
				
		       HeadObjectRequest headObjectRequest = HeadObjectRequest.builder ().bucket (this.bucketName).key (fileNameWithPrefix).build ();
		       HeadObjectResponse headObjectResponse = this.s3.headObject (headObjectRequest);
		       return headObjectResponse.sdkHttpResponse ().isSuccessful ();    
		   }
		   catch (NoSuchKeyException e) {
			   if(type == 0) {
				   logErrMsg(e.getLocalizedMessage());
			   }
		      return false;
		   }
		
	}

	
	
	@Override
	public String getBucketFileContent(String fileName, String bucketPrefix) {
		
		try {
			
		String fileNameWithPrefix = constructFileName(bucketPrefix, fileName);
		
		GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                .bucket(this.bucketName)
                .key(fileNameWithPrefix)
                .build();

	    ResponseInputStream<GetObjectResponse> responseInputStream = this.s3.getObject(getObjectRequest);
	    
	    

	    	InputStream stream = new ByteArrayInputStream(IOUtils.toByteArray(responseInputStream));
	       
	        String str = "";
	        int lineCount = 0;
	        StringBuffer buf = new StringBuffer(); 
	        BufferedReader reader = new BufferedReader(new InputStreamReader(stream));
	        if (stream != null) {                            
	            while ((str = reader.readLine()) != null) {    
	                buf.append(str + "\n" );
	                lineCount+=1;
	            }                
	        }
	       
	
	        return buf.toString(); 
	
	    } catch (IOException e) {
	    	logErrMsg(e.getLocalizedMessage());
	        return "4";
	    }catch (S3Exception e) {
	    	logErrMsg(e.getLocalizedMessage());
	        return "4";
	    } 
	}
	

	@Override
	public String getBucketFileLineCount(String fileName, String bucketPrefix) {
		try {
			
		String fileNameWithPrefix = constructFileName(bucketPrefix, fileName);
		
        GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                    .bucket(this.bucketName)
                    .key(fileNameWithPrefix)
                    .build();

        ResponseInputStream<GetObjectResponse> responseInputStream = this.s3.getObject(getObjectRequest);
        
        ////////////
        //ByteArrayInputStream bais= new ByteArrayInputStream(IOUtils.toByteArray(responseInputStream));
        //Scanner scanner = new Scanner(bais); 
        Scanner scanner = new Scanner(responseInputStream);
        
        //scanner.useDelimiter(Pattern.compile("[\\r\\n]+"));
        scanner.useDelimiter(Pattern.compile("(\\n)"));
        int countLine = 0;
        int emptyLineCount = 0;
        String strLine = "";
        
        logMsg("Getting Row Count...");
        while (scanner.hasNext()) {
        	strLine = scanner.next();
        	if(!strLine.trim().equals("")) {
        		countLine+=1;
        	}else {
        		emptyLineCount+=1;
        	}
        		
           }
           scanner.close();
           //logMsg("TBD..COUNT LINES:"+countLine);
           //logMsg("TBD..EMPTY LINES:"+emptyLineCount);        

            return Integer.toString(countLine);
            

        } 
//		catch (IOException e) {
//        	logErrMsg(e.getLocalizedMessage());
//            return "-1";
//        }
		catch (S3Exception e) {
        	logErrMsg(e.getLocalizedMessage());

            return "-1";
        } 


	}


	
	@Override
	public Integer moveFileWithinCloud(String sourceBucket, String sourceBucketPrefix, String targetBucket , String targetBucketPrefix, String fileName) {
		String encodedUrl = null;
	    try {
			if(!sourceBucketPrefix.equals("")) {
		        encodedUrl = URLEncoder.encode(sourceBucket + "/" + sourceBucketPrefix + "/" + fileName, StandardCharsets.UTF_8.toString());
			}
			else {
		        encodedUrl = URLEncoder.encode(sourceBucket + "/" + fileName, StandardCharsets.UTF_8.toString());
			}
	    } catch (UnsupportedEncodingException e) {
	        logErrMsg("URL could not be encoded: " + e.getMessage());
	    }
	    
	    //in destination bucket: u can either pass the same bucket name or pass another bucket name
	    //for destination key:
	    //					if u have passed same bucket name as source in destination bucket ,
	    //						then in destinationKey append a sub folder name before the fileName
	    //					if u have passed a different bucket in destinationbucket then,
	    //						no need to append a sub folder name in destinationKey, just fileName
	    
	    
	
	    
	    if(sourceBucket.equals(targetBucket) && sourceBucketPrefix.equals(targetBucketPrefix) ) {
	    	 logMsg("Landing and Loading bucket are same. No Prefix added or Prefixes are equal.");
	    	 return 11;
	    }
	    
	    String sourceFileName = constructFileName(sourceBucketPrefix, fileName);
	    String targetFileName = constructFileName(targetBucketPrefix, fileName);
	    
	    CopyObjectRequest copyReq = CopyObjectRequest.builder()
		            .copySource(encodedUrl)
		            .destinationBucket(targetBucket)
		            .destinationKey(targetFileName)
		            .build();


	    try {
	        CopyObjectResponse copyRes = s3.copyObject(copyReq);
	        logMsg(copyRes.copyObjectResult().toString());
	        
	        
	        ////Copy Validation steps
	        //match key, size and type of both the staging and copied/loaded file
	        HeadObjectRequest headObjectRequestStgFile = HeadObjectRequest.builder()
	    			  .bucket(sourceBucket)
	    			  .key(sourceFileName)
	    			  .build();
	        HeadObjectResponse headObjectResponseStgFile = s3.headObject(headObjectRequestStgFile);
	        
	        //logMsg("TBD..STAGE FILE...    "+"KEY:"+headObjectRequestStgFile.key()+"STG LEN:"+headObjectResponseStgFile.contentLength()+" , CONT TYPE:"+headObjectResponseStgFile.contentType()+ " , ETAG:"+headObjectResponseStgFile.eTag());
	        
	        HeadObjectRequest headObjectRequestLoadedFile = HeadObjectRequest.builder()
	    			  .bucket(targetBucket)
	    			  .key(targetFileName)
	    			  .build();
	        HeadObjectResponse headObjectResponseLoadedFile = s3.headObject(headObjectRequestLoadedFile);;
	        
	        //logMsg("TBD..LOAD FILE...    "+"KEY:"+headObjectRequestLoadedFile.key()+"STG LEN:"+headObjectResponseLoadedFile.contentLength()+" , CONT TYPE:"+headObjectResponseLoadedFile.contentType()+ " , ETAG:"+headObjectResponseLoadedFile.eTag());

	    	if (headObjectRequestStgFile.key().contains(fileName) && headObjectRequestLoadedFile.key().contains(fileName) 
	    			&& headObjectResponseStgFile.contentLength().equals(headObjectResponseLoadedFile.contentLength())
	    			&& headObjectResponseStgFile.contentType().equals(headObjectResponseLoadedFile.contentType())) { 
	    		
	    		//file is copied successfully, delete stg file
		        DeleteObjectRequest deleteObjectRequest = DeleteObjectRequest.builder()
		                .bucket(sourceBucket)
		                .key(sourceFileName)
		                .build();

		        DeleteObjectResponse delRes = this.s3.deleteObject(deleteObjectRequest);
		        
		        logMsg("File "+fileName+" removed successfully from cloud source bucket.");
		        //Validated Deletion (may not work due to issue of eventual consistency)
//		        if (checkBucketFileExists(fileName,1)) {
//		    		logMsg(fileName+ " file may not have been deleted from "+ sourceBucket+ " but has been copied to loading.");
//		        	return 11;
//		        }
//		        else {
//		        	logMsg("TBD File deleted");
//		        }
	    	}
	    	else {
	    		failedCopyFiles.add(fileName);
	    		logMsg("Error: Failed to copy file: "+fileName+" to cloud bucket: "+targetBucket);
	    		return 11;
	    	}
	    	/////
	        
	        return 0;
	    } catch (S3Exception e) {
	    	failedCopyFiles.add(fileName);
	    	logMsg("Error: Failed to copy file: "+fileName+" to cloud bucket: "+targetBucket);
	    	logErrMsg("Exception: "+e.awsErrorDetails().errorMessage());
	        return 11;
	    }
	  
	}
	
	
	
	
	//Upload file from local to cloud
	//PathWithFile = True means sourceDir path includes file name and does NOT need to be concatenated
	//PathWithFile = False means sourceDir path is separate from file name and needs to be concatenated
	public responseDTO uploadFileslocalToCloudUsingCommand(String sourceDir, String targetBucket , String targetBucketPrefix,String fileName, boolean pathWithFile,String targetFileName) {
		 String fromDir = "";
         String fileNameWithPrefix = "";
        
         if(pathWithFile == false) {
        	 if(!sourceDir.isEmpty()) {
            	 fromDir = sourceDir +"/"+ fileName;
              } else {
            	  fromDir = sourceDir;
              } 
         }
         else if (pathWithFile == true) {
        	 fromDir = sourceDir; //sourceDir has fullpath with file
         }
         
         if(targetBucketPrefix.equals("")) {
        	 fileNameWithPrefix = targetFileName.isEmpty() ? fileName : targetFileName;
         }
         else {
        	 if(targetBucketPrefix.endsWith("/")) {
        		 fileNameWithPrefix = targetBucketPrefix.concat(targetFileName.isEmpty() ? fileName : targetFileName);
                  
        	 }
        	 else {
        		 fileNameWithPrefix = targetBucketPrefix.concat("/").concat(targetFileName.isEmpty() ? fileName : targetFileName);      
        	 }
        }
                 
         try {
	         File fileToBeUploaded = new File(fromDir);
	         
	         PutObjectRequest putObjReq = PutObjectRequest.builder()
	        		 .bucket(targetBucket)
	        		 .key(fileNameWithPrefix)
	        		 .build();
	         this.s3.putObject(putObjReq, RequestBody.fromFile(fileToBeUploaded));
	         
	         //Delete file from local TODO
//	         if(fileToBeUploaded.exists()) {
//	        	 fileToBeUploaded.delete();
//	         }
	         //
	         
	         responseDTO r = new responseDTO();
	 			r.setReturnCode(0);
	 			r.setReturnMessage("File"+ fileName+" archived.");

	  	        return r;
         }
         catch(S3Exception e) {
        	
 	        responseDTO r = new responseDTO();
			r.setReturnCode(11);
			r.setReturnMessage("Exception: "+e.awsErrorDetails().errorMessage());

 	        return r;
         }
         catch(Exception e) {
         	
  	        responseDTO r = new responseDTO();
 			r.setReturnCode(11);
 			r.setReturnMessage("Exception: "+e.getLocalizedMessage());

  	        return r;
          }
        
	}
	
	
	
	public responseDTO moveBulkFilesWithinCloud(String sourceBucket, String sourceBucketPrefix, String targetBucket , String targetBucketPrefix, String profile,String fileName) {
		
		 String fromBucket = "";
         String toBucket = "";
         
         if(!sourceBucketPrefix.isEmpty()) {
         	sourceBucketPrefix = utils.standardizePrefixName(sourceBucketPrefix);
         	fromBucket = sourceBucket + "/" + sourceBucketPrefix;
         } else {
         	fromBucket = sourceBucket;
         }
         
         if(!targetBucketPrefix.isEmpty()) {
         	targetBucketPrefix = utils.standardizePrefixName(targetBucketPrefix);
         	toBucket = targetBucket + "/" + targetBucketPrefix;
         } else {
        	 toBucket = targetBucket;
         }
		
         /*
          * Sample Command:
          * aws s3 mv s3://gcfrlanding s3://gcfrlanding/landing --exclude "*" --include "CDR_2011-07-25_*.ctl" --recursive
          */
         
		String command = "aws s3 mv "+fromBucket+" "+toBucket+" --exclude \"*\" --include \""+fileName+"\" --profile "+profile+" --recursive";
		
		logMsg("command: "+command);
		
		try {
			return utils.executeScript(command);
		} catch (IOException | InterruptedException e) {
			responseDTO r = new responseDTO();
			r.setReturnCode(1);
			r.setReturnMessage("Exception:"+e.getMessage());
			
			return r;
		}		
	}
	
	
	@Override
	public String getBucketName() {
		return this.bucketName;
	}
	
		
	public String getExtensionByApacheCommonLib(String filename) {
		    return FilenameUtils.getExtension(filename);
	}
	
	//Validates prefix and constructs filename accordingly
	// filePrefix: company/date/day + fileName: abc.txt -> company/date/day/abc.txt
	public String constructFileName(String filePrefix, String fileName) {
		String resFileName = "";
		
		if(!filePrefix.equals("")) {
			if(!filePrefix.endsWith("/")) {
				resFileName = filePrefix.concat("/").concat(fileName);
			}
			else {
				resFileName = filePrefix.concat(fileName);
			}
	    }
	    else { 
	    	resFileName = fileName;
	    }
		return resFileName;
	}
	
	private void initProccesLoggerInstance(requestDTO request) {
		processLogger = CustomUtils.customLogger.get(request.getProcess());
		if(processLogger==null) {
			logger.warn("Job: "+request.getJob()+", " + request.getProcess()+".log -> logger instance failed to register!!! \n All logs will be sent to application main log if registered in application.properties" );
		}
		
		if(utils!=null) {
			utils.setprocLogger(request.getProcess());
		}
	}
	
	
	
	private void logMsg(String msg) {
		if(this.processLogger!=null) {
			processLogger.info(msg);
		} else {
			logger.info(msg);
		}
	}
	
	private void logErrMsg(String msg) {
		if(this.processLogger!=null) {
			processLogger.log(Level.WARNING, msg);
		} else {
			logger.error(msg);
		}
	}


	@Override
	public boolean checkCustomFileExists(String fileName, String bucket, String bucketPrefix, Integer type) {
		try {	
			String fileNameWithPrefix = constructFileName(bucketPrefix, fileName);
			
	       HeadObjectRequest headObjectRequest = HeadObjectRequest.builder ().bucket (bucket).key (fileNameWithPrefix).build ();
	       HeadObjectResponse headObjectResponse = this.s3.headObject (headObjectRequest);
	       return headObjectResponse.sdkHttpResponse ().isSuccessful ();    
	   }
	   catch (NoSuchKeyException e) {
		   if(type == 0) {
			   logErrMsg(e.getLocalizedMessage());
		   }
	      return false;
	   }
	}


	@Override
	public Integer renameAndmoveWithinCloud(String sourceBucket, String sourceBucketPrefix, String targetBucket,
			String targetBucketPrefix, String fileName, String targetfilename) {
		String encodedUrl = null,prefixedFileName="";
		
	    try {
			if(!sourceBucketPrefix.equals("")) {
				prefixedFileName = sourceBucketPrefix + "/" + fileName;
		        encodedUrl = URLEncoder.encode(sourceBucket + "/" + sourceBucketPrefix + "/" + fileName, StandardCharsets.UTF_8.toString());
			}
			else {
				prefixedFileName = fileName;
		        encodedUrl = URLEncoder.encode(sourceBucket + "/" + fileName, StandardCharsets.UTF_8.toString());
			}
	    } catch (UnsupportedEncodingException e) {
	        logErrMsg("URL could not be encoded: " + e.getMessage());
	    }
	    
	    //in destination bucket: u can either pass the same bucket name or pass another bucket name
	    //for destination key:
	    //					if u have passed same bucket name as source in destination bucket ,
	    //						then in destinationKey append a sub folder name before the fileName
	    //					if u have passed a different bucket in destinationbucket then,
	    //						no need to append a sub folder name in destinationKey, just fileName
	    
	    
	
	    
	    if(sourceBucket.equals(targetBucket) && sourceBucketPrefix.equals(targetBucketPrefix) ) {
	    	 logMsg("Landing and Loading bucket are same. No Prefix added or Prefixes are equal.");
	    	 return 11;
	    }
	    
	    String sourceFileName = constructFileName(sourceBucketPrefix, fileName);
	    String targetFileName = constructFileName(targetBucketPrefix, targetfilename.isEmpty() ? fileName : targetfilename);
	    
	    CopyObjectRequest copyReq = CopyObjectRequest.builder()
		            .copySource(encodedUrl)
		            .destinationBucket(targetBucket)
		            .destinationKey(targetFileName)
		            .build();


	    try {
	        CopyObjectResponse copyRes = s3.copyObject(copyReq);
	        logMsg(copyRes.copyObjectResult().toString());
	        
	        ////Copy Validation steps
	        S3Object s3_sourceObject = this.listcustomBucketObjects(sourceBucket,prefixedFileName);
	        S3Object s3_targetObject = this.listcustomBucketObjects(targetBucket,targetFileName);
	        
	        //logMsg("tbd..s3_sourceObject.size(): "+s3_sourceObject.size());
	        //logMsg("tbd..s3_targetObject.size(): "+s3_targetObject.size());
	        
	        if(s3_targetObject!=null) {
	        	//match size and compare modified dates of both the staging and copied/loaded file	        	
	        	if(!s3_sourceObject.size().equals(s3_targetObject.size())) {
	        		failedCopyFiles.add(fileName);
	        		logMsg("Error: File \"+fileName+\" copied to cloud bucket: "+targetfilename + " with exception their sizes mismatched.");
		    		return 11;
		        }
	        	
	        	 if(s3_sourceObject.lastModified().isAfter(s3_targetObject.lastModified())) {
	        		 failedCopyFiles.add(fileName);
		        		logMsg("Error: File "+fileName+" copied to cloud bucket: "+targetfilename + " with exception source file lastmodified is later then copied target file.");
			    		return 11;
	 	        }
	        	 
	        } else {
	    		failedCopyFiles.add(fileName);
	    		logMsg("Error: Failed to copy file: "+fileName+" to cloud bucket: "+targetfilename);
	    		return 11;
	    	}
	       
	        DeleteObjectRequest deleteObjectRequest = DeleteObjectRequest.builder()
	                .bucket(sourceBucket)
	                .key(sourceFileName)
	                .build();

	        DeleteObjectResponse delRes = this.s3.deleteObject(deleteObjectRequest);
	        
	        s3_sourceObject = this.listcustomBucketObjects(sourceBucket,prefixedFileName);
	        
	        if (s3_sourceObject!=null) {
	    		logMsg("Warning: "+fileName+ " file may not have been deleted from "+ sourceBucket+ " but it has been copied to loading.");
	        }
	        else {
	        	logMsg("File "+fileName+" copied successfully to target bucket.");
	        }
	        	        
	        return 0;
	    } catch (S3Exception e) {
	    	failedCopyFiles.add(fileName);
	    	logMsg("Error: Failed to copy file: "+fileName+" to cloud bucket: "+targetBucket);
	    	logErrMsg("Exception: "+e.awsErrorDetails().errorMessage());
	        return 11;
	    }
	}


	@Override
	public String returnBucketName(String bucketUrl) {
		// TODO Auto-generated method stub
        return splitBucketUrl(bucketUrl, tempStartStr);
	}
}
