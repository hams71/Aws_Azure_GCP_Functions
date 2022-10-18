
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.CopyWriter;
//import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.common.collect.Lists;
import com.teradata.gcfr.common.interfaces.GCFR_Cloud_Storage_Int;
import com.teradata.gcfr.common.model.CustomUtils;
import com.teradata.gcfr.common.model.requestDTO;
import com.teradata.gcfr.common.model.responseDTO;

import software.amazon.awssdk.services.s3.model.ListObjectsRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.S3Object;

import com.google.api.gax.paging.Page;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.ReadChannel;
import com.google.cloud.storage.Blob;


import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.channels.Channels;
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


public class GCFR_Cloud_Storage_GCP_Impl implements GCFR_Cloud_Storage_Int {

	org.slf4j.Logger logger = LoggerFactory.getLogger(GCFR_Cloud_Storage_GCP_Impl.class);
	responseDTO response = null;
	private Logger processLogger;
	private CustomUtils utils;
	
	private String CLOUD_PROFILE; //gcfr-testing-service-acc@gcfr-testing-ha.iam.gserviceaccount.com
	private String LANDING_DIRECTORY; //gs://gcfr-testing
	
	private String bucketName;
	String projectId;
	LinkedHashMap<String, String> objectNames;
	private String jsonKeyPath;
	private String tempStartStr = "gs://";
	
	Storage storage;
	
	public List<String> failedCopyFiles;
	
	
	public GCFR_Cloud_Storage_GCP_Impl(String CP, String LD, String JP,requestDTO request){
		this.CLOUD_PROFILE = CP;
		this.LANDING_DIRECTORY = LD;
		this.jsonKeyPath = JP;
		
		
		failedCopyFiles = new ArrayList<>();
		
		utils = new CustomUtils();
		initProccesLoggerInstance(request);
	}
	
	@Override
	public Integer aunthenticateIntoCloud() {
		
		GoogleCredentials credentials;
		try {
			credentials = GoogleCredentials.fromStream(new FileInputStream(jsonKeyPath))
			          .createScoped(Lists.newArrayList("https://www.googleapis.com/auth/cloud-platform"));
			
			
			 //The ID of your GCP project
//			projectId = extractGcpProjectId(CLOUD_PROFILE);
//			if (projectId.equals("-1")) {
//				logMsg("GCP Profile/ServiceAccount might not be entered correctly.");
//				return 13;
//			}
	    
	    
	        //storage = StorageOptions.newBuilder().setProjectId(projectId).build().getService();
		    storage = StorageOptions.newBuilder().setCredentials(credentials).build().getService();

	        
		    ///////
		    /*
		    String tempStartStr = "gs://";
		    splitBucketUrl(LANDING_DIRECTORY, tempStartStr);
		    */
		    
		    this.bucketName =  this.returnBucketName(LANDING_DIRECTORY);  
		    
		    //substring after last slash(/), gs://gcfr-testing
	        //this.bucketName = LANDING_DIRECTORY.substring(LANDING_DIRECTORY.lastIndexOf("/") + 1);//"gcfr-testing";
	       
	        
	        if(checkBucketExists(this.bucketName) == false) {
	        	//logMsg("GCP Bucket does not exist.");
	        	return 13;
	        }
	        
	        logMsg("GCP Bucket "+ bucketName+ " exists.");
	        
	        
	        //Linked Hash Map used to preserve ordering of elements
	        // name, Value is type of object e.g Control file or Data file
	        this.objectNames = new LinkedHashMap<>();

			
		} catch (FileNotFoundException e) {
			logErrMsg(e.getLocalizedMessage());
			return 13;
		} catch (IOException e) {
			logErrMsg(e.getLocalizedMessage());
			return 13;
		}
	      
		return 0;    
		
	}

	//to split bucket url into only bucket name 
    //gs://gcfrtestbuck/loading -> gcfrtestbuck/loading
	@Override
	public String splitBucketUrl(String bucketUrl, String tempStartStr) {
    	String returnBucketName = "";
    	 
    	if (bucketUrl.startsWith(tempStartStr)) {
    		if(bucketUrl.endsWith("/")) {
          		//remove the "gs://" and remove the last slash "/" 
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
	
	@Override
	public LinkedHashMap<String, String> listBucketObjects() {
		
		try {
			//Get List of Blobs
	        Page<Blob> blobs = this.storage.list(this.bucketName);

	        for (Blob blob : blobs.iterateAll()) {
	            //logMsg("TBD..The name of the object is "+blob.getName());
	            
	            //Save file name and its extension
	            this.objectNames.put(blob.getName(), getExtensionByApacheCommonLib(blob.getName()));
	        }
		}
		
        catch (Exception e) {
			 logErrMsg("GCP ERROR:"+e.getLocalizedMessage());
        }
		return this.objectNames;
	}
	
	
	//Lists only specific objects within bucket
	public  Blob listcustomBucketObjects(String bucketName,String Prefix) {
		try {
			  Page<Blob> blobs =
				        this.storage.list(
				            bucketName,
				            Storage.BlobListOption.prefix(Prefix),
				            Storage.BlobListOption.currentDirectory());

				    for (Blob blob : blobs.iterateAll()) {
				      //logMsg("TBD list object...."+blob.getName());
				      return blob;
				    }
				   
		}
		
        catch (Exception e) {
			 logErrMsg("GCP ERROR:"+e.getLocalizedMessage());
			 return null;
        }
		//logMsg("No Object found");
		 return null;
		
	}

	@Override
	public boolean checkBucketFileExists(String fileName,String bucketPrefix, Integer type) {
		try {
			String fileNameWithPrefix = constructFileName(bucketPrefix, fileName);
			
			Blob blob = this.storage.get(this.bucketName,fileNameWithPrefix);
	        if (blob != null && blob.exists()){
	        	return true;
	        } 
	        else {
	        	return false;
	        }
		}
        catch(Exception e) {
        	 if(type == 0) {
				   logErrMsg(e.getLocalizedMessage());
			 }
        	 return false;
        }
		
	}

	
	///Check file exists in specific bucket
	@Override
	public boolean checkCustomFileExists(String fileName, String bucket, String bucketPrefix, Integer type) {
		try {
			String fileNameWithPrefix = constructFileName(bucketPrefix, fileName);
			
			Blob blob = this.storage.get(bucket,fileNameWithPrefix);
	        if (blob != null && blob.exists()){
	        	return true;
	        } 
	        else {
	        	return false;
	        }
		}
        catch(Exception e) {
        	 if(type == 0) {
				   logErrMsg(e.getLocalizedMessage());
			 }
        	 return false;
        }
	}
	
	
	@Override
	public String getBucketFileContent(String fileName, String bucketPrefix) {
		try{
			String fileNameWithPrefix = constructFileName(bucketPrefix, fileName);
			
			Blob blob = this.storage.get(this.bucketName,fileNameWithPrefix);
			 if (blob != null  && blob.exists()) {
				 String fileContent = new String(blob.getContent());
				 return fileContent;
			 }
			 else {
				 return "4";
			 }
		}
		catch(Exception e) {
			 logErrMsg(e.getLocalizedMessage());
			return "4";
		}
		
	}

	@Override
	public boolean checkBucketExists(String bucketName) {
		try {
			boolean retVar = storage.get(bucketName, Storage.BucketGetOption.fields()) != null;
			if (retVar == false) {
				logMsg("GCP Bucket Error. Either bucket does not exist or bucket premissions issue.");
			}
			return retVar;
		}
		catch (Exception e) {
			logErrMsg(e.getLocalizedMessage());
			return false;
		}
		
	}

	@Override
	public String getBucketFileLineCount(String fileName, String bucketPrefix) {
		
		try {
			String fileNameWithPrefix = constructFileName(bucketPrefix, fileName);
			
			Blob blob = this.storage.get(this.bucketName,fileNameWithPrefix);
			
			ReadChannel readerC;
	
	        String str = "";
	        Integer lineCount = 0;
	        
	        ///
	        int countLine = 0;
            int emptyLineCount = 0;
            String strLine = "";
	        ///
	
	        if (blob != null  && blob.exists()) {
	            readerC = blob.reader();
	            InputStream inputStream = Channels.newInputStream(readerC);
	            
	            ////////////
	            //ByteArrayInputStream bais= new ByteArrayInputStream(IOUtils.toByteArray(inputStream));
	            //Scanner scanner = new Scanner(bais); 
	            Scanner scanner = new Scanner(inputStream); 
	            
	            //scanner.useDelimiter(Pattern.compile("[\\r\\n]+"));
	            scanner.useDelimiter(Pattern.compile("(\\n)"));
	            
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
	
	        }
	        else {
	        	return "-1";
	        }
	
	        //return lineCount.toString();
	        return Integer.toString(countLine);
        
		}
		catch (Exception e) {
        	logErrMsg(e.getLocalizedMessage());
            return "-1";
        } 
	}

	
	//move file from one bucket to another of same project (buck1->file.txt to buck2->file.txt)
	// or move file within same bucket to a different folder (buck1/land/file.txt to buck1/load/file.txt)
	@Override
	public Integer moveFileWithinCloud(String sourceBucket, String sourceBucketPrefix, String targetBucket , String targetBucketPrefix, String fileName) {
			
			try {
				
				if(sourceBucket.equals(targetBucket) && sourceBucketPrefix.equals(targetBucketPrefix) ) {
			    	 logMsg("Landing and Loading bucket are same. No Prefix added or Prefixes are equal.");
			    	 return 11;
			    }
			    
				String sourceFileNameWithPrefix = constructFileName(sourceBucketPrefix, fileName);
				
				//Blob blob = this.storage.get(sourceBucket, fileName);
			     Blob blob = this.storage.get(sourceBucket, sourceFileNameWithPrefix);

				String targetFileNameWithPrefix = constructFileName(targetBucketPrefix, fileName);
	
			    // Write a copy of the object to the target bucket
			    CopyWriter copyWriter = blob.copyTo(targetBucket, targetFileNameWithPrefix);
			    Blob copiedBlob = copyWriter.getResult();
			    
			    //Check if blob successfully copied
			    boolean exists = copiedBlob.exists();
			    if (exists) {
			    	
			    	// Delete the original blob now that we've copied to where we want it, finishing the "move"
				    // operation
			    	blob.delete();
			    	
			    	//logMsg("TBD..GCP Moved object "+ fileName + " from bucket " + sourceBucket+ " to "+ fileName   + " in bucket " + copiedBlob.getBucket());		
			    	logMsg("File "+ fileName + " removed successfully from cloud source bucket.");		  


					return 0;
			    } 
			    else {
			    	// the blob was not found
			    	logMsg("File not copied/moved successfully");
			    	return 11;
			    }

				
		    }  catch (Exception e) {
		        logErrMsg("Error: "+e.getLocalizedMessage());
		        return 11;
		    }
	 
		}
	
	
	//Move multiple files from one bucket to another using pattern , uses shell command
	//Sample Command: gsutil mv gs://gcfr-loading/CDR_2011-07-25_*.ctl gs://gcfr-landing/landingB
	@Override
	public responseDTO moveBulkFilesWithinCloud(String sourceBucket, String sourceBucketPrefix, String targetBucket,
			String targetBucketPrefix, String profile, String fileName) {
		
		String fromBucket = "";
		String toBucket = "";
		
		if(!sourceBucketPrefix.isEmpty()) {
			sourceBucketPrefix = utils.standardizePrefixName(sourceBucketPrefix);
			fromBucket = sourceBucket + "/" + sourceBucketPrefix + "/" + fileName;
		} else {
			fromBucket = sourceBucket + "/" + fileName;
		}
		
		if(!targetBucketPrefix.isEmpty()) {
			targetBucketPrefix = utils.standardizePrefixName(targetBucketPrefix);
			toBucket = targetBucket + "/" + targetBucketPrefix;
		} else {
			toBucket = targetBucket;
		}
	   
		
		//authenticate with the service account
		String authCommand = "gcloud auth activate-service-account --key-file "+jsonKeyPath;

		try {
			logMsg("command executed: "+authCommand);
			responseDTO retAuthCommand = utils.executeScript(authCommand);

			if(retAuthCommand.getReturnCode() == 0){
				
				//String command = "gsutil -m mv "+fromBucket+" "+toBucket;//-m for parallel uploads
				String command = "gsutil mv "+fromBucket+" "+toBucket;
	   
				logMsg("command executed: "+ command);
				
				responseDTO returnObjMove = utils.executeScript(command);
				
				//logout from service account
//				if(returnObjMove.getReturnCode() == 0){
//					String commandLogout = "gsutil auth revoke "; //get service account name here  
//					logMsg("command executed: "+ commandLogout);
//					return utils.executeScript(commandLogout);
//				}
				 return returnObjMove;
			}
			else{
				logMsg("GCP Authentication unsuccessfull."+retAuthCommand.getReturnMessage());
				return retAuthCommand;
			}
	   
		} catch (IOException | InterruptedException e) {
			logMsg("Exception:"+e.getMessage());
			responseDTO r = new responseDTO();
			r.setReturnCode(1);
			r.setReturnMessage("Exception:"+e.getMessage());
			
			return r;
	   }
	}
	
	
	
	//Upload file from local system to cloud, uses shell command
	//PathWithFile = True means sourceDir path includes file name and does NOT need to be concatenated
	//PathWithFile = False means sourceDir path is separate from file name and needs to be concatenated
	//sample : gsutil mv "C:\Users\User\Documents\CDR_2011*" gs://gcfr-landing
	@Override
	public responseDTO uploadFileslocalToCloudUsingCommand(String sourceDir, String targetBucket,
			String targetBucketPrefix, String fileName,boolean pathWithFile,String targetFileName) {
		
		 String fromDir = "";
         String fileNameWithPrefix = "";
        
         if(pathWithFile == false) {
        	 if(!sourceDir.isEmpty()) {
            	 fromDir = sourceDir + "/" + fileName;
              } else {
            	  fromDir = sourceDir;
              } 
         }
         else if (pathWithFile == true) {
        	 fromDir = sourceDir; //sourceDir has fullpath with file
         }
         
         if(!targetBucket.startsWith("gs://")) {
        	 targetBucket = "gs://".concat(targetBucket);
         }
         
         if(targetBucketPrefix.equals("")) {
        	 fileNameWithPrefix = fileName;
         }
         else {
        	 if(targetBucketPrefix.endsWith("/")) {
        		 fileNameWithPrefix = targetBucketPrefix.concat(targetFileName.isEmpty() ? fileName : targetFileName);  
        	 }
        	 else {
        		 fileNameWithPrefix = targetBucketPrefix.concat("/").concat(targetFileName.isEmpty() ? fileName : targetFileName);      
        	 }
        	 //fileNameWithPrefix = targetBucketPrefix.concat("/").concat(fileName);
         }


       //authenticate with the service account
        String authCommand = "gcloud auth activate-service-account --key-file "+jsonKeyPath;
        
 		try {
 			logMsg("command executed: "+authCommand);
 			responseDTO retAuthCommand = utils.executeScript(authCommand);

 			if(retAuthCommand.getReturnCode() == 0){
 				String command = "gsutil mv ".concat(fromDir).concat(" ")
 						.concat(targetBucket).concat("/").concat(fileNameWithPrefix);
 	   
 				logMsg("command executed: "+command);
 				 return utils.executeScript(command);
 			}
 			else{
 				logMsg("GCP Authentication unsuccessfull."+retAuthCommand.getReturnMessage());
 				return retAuthCommand;
 			}
 	   
 		} catch (IOException | InterruptedException e) {
 			responseDTO r = new responseDTO();
 			r.setReturnCode(1);
 			r.setReturnMessage("Exception:"+e.getMessage());
 			
 			return r;
 	   }
	}
	
	
	
	@Override
	public Integer renameAndmoveWithinCloud(String sourceBucket, String sourceBucketPrefix, String targetBucket,
			String targetBucketPrefix, String fileName, String targetfilename) {
		
		String prefixedFileName="";
		try {
			if(!sourceBucketPrefix.equals("")) {
				prefixedFileName = sourceBucketPrefix + "/" + fileName;
			}
			else {
				prefixedFileName = fileName;
			}
			
			
			if(sourceBucket.equals(targetBucket) && sourceBucketPrefix.equals(targetBucketPrefix) ) {
		    	 logMsg("Landing and Loading bucket are same. No Prefix added or Prefixes are equal.");
		    	 return 11;
		    }
			
			String sourceFileName = constructFileName(sourceBucketPrefix, fileName);
		    String targetFileName = constructFileName(targetBucketPrefix, targetfilename.isEmpty() ? fileName : targetfilename);
		    
		    
		     Blob blob = this.storage.get(sourceBucket, sourceFileName);

		    // Write a copy of the object to the target bucket
		    CopyWriter copyWriter = blob.copyTo(targetBucket, targetFileName);
		    Blob copiedBlob = copyWriter.getResult();
		    
		    //Check if blob successfully copied
		    boolean exists = copiedBlob.exists();
		    if (exists) {
		        ////Copy Validation steps
		        Blob gcp_sourceObject = this.listcustomBucketObjects(sourceBucket,prefixedFileName);
		        Blob gcp_targetObject = this.listcustomBucketObjects(targetBucket,targetFileName);
		        
		        //logMsg("tbd..gcp_sourceObject.size(): "+gcp_sourceObject.getSize());
		        //logMsg("tbd..gcp_targetObject.size(): "+gcp_targetObject.getSize());
		        
		        if(gcp_targetObject!=null) {
		        	//match size and compare modified dates of both the staging and copied/loaded file	        	
		        	if(!gcp_sourceObject.getSize().equals(gcp_targetObject.getSize())) {
		        		failedCopyFiles.add(fileName);
		        		logMsg("Error: File \"+fileName+\" copied to cloud bucket: "+targetfilename + " with exception their sizes mismatched.");
			    		return 11;
			        }
		        	
		        	 //if(gcp_sourceObject.getUpdateTime().isAfter(gcp_targetObject.getUpdateTime())) {
		        	if(gcp_sourceObject.getUpdateTime().compareTo(gcp_targetObject.getUpdateTime()) > 0 ) {
		        		 failedCopyFiles.add(fileName);
			        		logMsg("Error: File "+fileName+" copied to cloud bucket: "+targetfilename + " with exception source file lastmodified is later then copied target file.");
				    		return 11;
		 	        }
		        	 
		        } else {
		    		failedCopyFiles.add(fileName);
		    		logMsg("Error: Failed to copy file: "+fileName+" to cloud bucket: "+targetfilename);
		    		return 11;
		    	}
		        
		       
		    	// Delete the original blob now that we've copied to where we want it, finishing the "move"
			    // operation
		    	blob.delete();
		    	
		    	 gcp_sourceObject = this.listcustomBucketObjects(sourceBucket,prefixedFileName);
			        
			     if (gcp_sourceObject!=null) {
			   		logMsg("Warning: "+fileName+ " file may not have been deleted from "+ sourceBucket+ " but it has been copied to loading.");
			     }
			     else {
			      	logMsg("File "+fileName+" copied successfully to target bucket.");
			     }
			        	    

				return 0;
		    } 
		    else {
		    	failedCopyFiles.add(fileName);
	    		logMsg("Error: Failed to copy file: "+fileName+" to cloud bucket: "+targetfilename);
	    		return 11;
		    }
		}
		
		catch(Exception e) {
			logErrMsg("Exception: "+e.getLocalizedMessage());
			return 11;
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
	
	//Extract Prokect Id from Service Account Url
	//gcfr-testing-service-acc@gcfr-testing-ha.iam.gserviceaccount.com
	//gcfr-testing-ha
	String extractGcpProjectId(String url){
		try {
			String[] splitArr = url.split("@");
			
			if(splitArr.length > 1) {
				String afterAtStr = splitArr[1];
				String[] proIdExtractedArr = afterAtStr.split("\\.");		
				return proIdExtractedArr[0];

			}
			return url;	
		}
		catch(Exception e) {
			logErrMsg(e.getLocalizedMessage());
			return "-1";
		}
		
	}
	
	//////////LOG FUNCs
	
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
	public String returnBucketName(String bucketUrl) {
		// TODO Auto-generated method stub
	    return splitBucketUrl(bucketUrl, tempStartStr);
	}

	

	



}
