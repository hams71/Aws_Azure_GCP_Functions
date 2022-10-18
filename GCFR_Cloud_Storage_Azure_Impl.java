
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;
import org.simpleframework.xml.core.Replace;
import org.slf4j.LoggerFactory;
import org.spongycastle.util.Arrays.Iterator;

import com.azure.core.exception.AzureException;
import com.azure.core.http.rest.PagedIterable;
import com.azure.core.util.polling.PollResponse;
import com.azure.core.util.polling.SyncPoller;
import com.azure.identity.ClientSecretCredential;
import com.azure.identity.ClientSecretCredentialBuilder;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.models.BlobCopyInfo;
import com.azure.storage.blob.models.BlobItem;
import com.azure.storage.blob.models.BlobProperties;

// Import the interface here 

public class GCFR_Cloud_Storage_Azure_Impl implements GCFR_Cloud_Storage_Int {

	org.slf4j.Logger logger = LoggerFactory.getLogger(GCFR_Cloud_Storage_Azure_Impl.class);
	responseDTO response = null;
	private Logger processLogger;
	private CustomUtils utils;
	
	private String CLOUD_PROFILE; //'EX_100_100_CUSTOMER_AWS.azure
	private String LANDING_DIRECTORY; //https://gcfrtestacc.blob.core.windows.net/gcfrtestcont
	private String LOGON_FILE_PATH; //C:/GCFR_Root/logon/
	
	private LinkedHashMap<String, String> objectNames;
	
	private ClientSecretCredential clientSecretCredential;
	private BlobServiceClient blobServiceClient;
	private String containerName;
	private BlobContainerClient containerClient;
	
	public List<String> failedCopyFiles;
	
	public GCFR_Cloud_Storage_Azure_Impl(String CP, String LD, String LFP, requestDTO request){
		
		this.CLOUD_PROFILE = CP;
		this.LANDING_DIRECTORY = LD;
		this.LOGON_FILE_PATH = LFP;
		
		
		utils = new CustomUtils();
		failedCopyFiles = new ArrayList<>();
		initProccesLoggerInstance(request);
	}
	
	
	private responseDTO authenticateAzureCLI() {
		
		String data = null;
		String[] arr = null;
		
		String ClientId = null;
		String ClientSecret = null;
		String TenantId = null;
		
		responseDTO res = null;
		
		// reading the .azure file for credentials
		// which has ClientId, ClientSecret, TenantId which we read
		try {
			  //System.out.println(CLOUD_PROFILE); // the file name
		      File myObj = new File(LOGON_FILE_PATH+CLOUD_PROFILE); //the location + the file name which has the credentials for azure 
		      Scanner myReader = new Scanner(myObj);
		      while (myReader.hasNextLine()) {
		        data = myReader.nextLine();
		        arr = data.split("=");
		        if (arr[0].startsWith("ClientId")) {
		        	ClientId = arr[1].trim();
		        }
		        else if (arr[0].startsWith("ClientSecret")) {
		        	ClientSecret = arr[1].trim();
		        }
		        else if (arr[0].startsWith("TenantId")) {
		        	TenantId= arr[1].trim();
		        }
		      }
              
		      // closing the file
		      myReader.close();
		      
		    } 
		catch (FileNotFoundException e) {
			logMsg(e.getLocalizedMessage());
		    }
		
		
		try {
			String command = "az login --service-principal --username \""+ClientId+"\" --tenant \""+TenantId+"\" --password \""+ClientSecret+"\"";
			//logMsg("TBD COMMAND "+ command);
			res = utils.executeScript(command);
			//logMsg("TBD Return Code "+ res.getReturnCode());
			if(res.getReturnCode()!=0) {
				return res;
			}
		} catch (Exception e) {
			e.printStackTrace();
			res.setReturnCode(1);
			res.setReturnMessage("Exception:"+e.getMessage());
			return res;
		}
		return res;
	}
	
	private responseDTO logoutAzureCLI() {
		
		responseDTO res = null;
		
		try {
			String command = "az logout";
			//logMsg("TBD COMMAND "+ command);
			res = utils.executeScript(command);
			//logMsg("TBD Return Code "+ res.getReturnCode());
			if(res.getReturnCode()!=0) {
				return res;
			}
		} catch (Exception e) {
			e.printStackTrace();
			res.setReturnCode(1);
			res.setReturnMessage("Exception:"+e.getMessage());
			return res;
		}
		return res;
	}
	
	
	public Integer aunthenticateIntoCloud() {
		
		String data = null;
		String[] arr = null;
		
		String loadingUrl = null;
		
		String ClientId = null;
		String ClientSecret = null;
		String TenantId = null;
		
		// reading the .azure file for credentials
		// which has ClientId, ClientSecret, TenantId which we read
		try {
			  //System.out.println(CLOUD_PROFILE); // the file name
		      File myObj = new File(LOGON_FILE_PATH+CLOUD_PROFILE); //the location + the file name which has the credentials for azure 
		      Scanner myReader = new Scanner(myObj);
		      while (myReader.hasNextLine()) {
		        data = myReader.nextLine();
		        arr = data.split("=");
		        if (arr[0].startsWith("ClientId")) {
		        	ClientId = arr[1].trim();
		        }
		        else if (arr[0].startsWith("ClientSecret")) {
		        	ClientSecret = arr[1].trim();
		        }
		        else if (arr[0].startsWith("TenantId")) {
		        	TenantId= arr[1].trim();
		        }
		      }
              
		      // closing the file
		      myReader.close();
		      
		    } 
		catch (FileNotFoundException e) {
			logMsg(e.getLocalizedMessage());
			return 13;
		    }
		
		
		 this.containerName = this.returnBucketName(LANDING_DIRECTORY);
		 logMsg("Azure Container Name: "+ containerName);
		 	    
		 loadingUrl = this.returnBucketEndPointURL(LANDING_DIRECTORY);
		 
		 /*
		  // removing the last / if any from the provided string 
		
		 // gcfrbucket/ -> gcfrbucket
		 String returnLandingDirectory = removeLastChar(LANDING_DIRECTORY);
		 // getting the container name from the URL 
		 this.containerName = returnLandingDirectory.substring(returnLandingDirectory.lastIndexOf("/") + 1); //gcfr
<<<<<<< .mine
	     //logMsg("Azure Container Name: "+ containerName);


=======
		 
		 //loadingUrl = returnLandingDirectory.substring(0,returnLandingDirectory.lastIndexOf("/") + 1);
		  */	     

	     
		 this.clientSecretCredential = new ClientSecretCredentialBuilder()
			     .clientId(ClientId)
			     .clientSecret(ClientSecret)
			     .tenantId(TenantId)
			     .build();
		 
         this.blobServiceClient = new BlobServiceClientBuilder()
                 .endpoint(loadingUrl)
                 .credential(clientSecretCredential)
                 .buildClient();

         
         // Get Container Client
         this.containerClient = blobServiceClient.getBlobContainerClient(containerName);
        
         
         //blob Names are also called Keys
         //Linked Hash Map used to preserve ordering of elements
         //Key is name, Value is type of blob e.g Control file or Data file
         
         if(checkBucketExists(this.containerName) == false) {
          	return 13;
          }
         logMsg("Azure bucket/landing bucket "+ this.containerName+" exists.");
         
         this.objectNames = new LinkedHashMap<>();
         
         return 0;
	}
	
	// bucketName -> gcfrbucket
	public boolean checkBucketExists(String bucketName) {
		
		// to check if the container exists or not 
		 try {
			    BlobContainerClient containerClient = blobServiceClient.getBlobContainerClient(bucketName);
			 	boolean exist = containerClient.exists();
			 	if(!exist) {
			 		logMsg(" Azure bucket " +bucketName+" does not exist or storage access issue..");
			 	}
		        return exist;
		    }  
		    catch (Exception e) {
		    	logErrMsg(" Azure Container does not exist or storage access issue.."+ e.getLocalizedMessage());
		        return false;
		    }
	}


	// Storing it with the name of the file and also the extension
	// The Extension is then used in verify to check suffix for filtering
	public LinkedHashMap<String, String> listBucketObjects() {

           //Iterating and getting list of blobs
        for (BlobItem blobItem : this.containerClient.listBlobs()) {
            //logMsg("TBD..The name of the object is " + blobItem.getName());
                this.objectNames.put(blobItem.getName(),getExtensionByApacheCommonLib(blobItem.getName()));
           }
		return objectNames;
	}

	
	public boolean checkBucketFileExists(String fileName, String bucketPrefix ,Integer type) {
		
		try {	
			// need to append the bucketName with fileName
			String fileNameWithPrefix = constructFileName(bucketPrefix, fileName);
			BlobClient blob = this.containerClient.getBlobClient(fileNameWithPrefix);
			return blob.exists();
		}
		catch(Exception e){
			logMsg("Azure Blob does not exist or storage access issue "+ e.getLocalizedMessage());
			return false;
		}
	}
	
	
	@Override
	public String getBucketFileContent(String fileName, String bucketPrefix) {

		try {
			String fileNameWithPrefix = constructFileName(bucketPrefix, fileName);
            BlobClient blob = this.containerClient.getBlobClient(fileNameWithPrefix);
            
            String str = "";
            
            InputStream inputStream = blob.getBlockBlobClient().openInputStream();
            StringBuffer buf = new StringBuffer(); 
            BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
            if (inputStream != null) {                            
                try {
                    while ((str = reader.readLine()) != null) {    
                        buf.append(str + "\n" );
                    }
                    
                }
                catch (IOException e) {
                	logErrMsg(e.getLocalizedMessage());
                    return "4";
                } 
                catch (AzureException e) {
                	logErrMsg(e.getLocalizedMessage());
                	return "4";
                }
            }
            return buf.toString();
            
		}
		catch(AzureException e) {
			logErrMsg(e.getLocalizedMessage());
			return "4";
		}
	}
	
	
	@Override
	public String getBucketFileLineCount(String fileName, String bucketPrefix) {
		
		String str = "";
        int lineCount = 0;
        
		try {
			String fileNameWithPrefix = constructFileName(bucketPrefix, fileName);
            BlobClient blob = this.containerClient.getBlobClient(fileNameWithPrefix); 
            
            
            InputStream inputStream = blob.getBlockBlobClient().openInputStream();
            
            ////////////
            //ByteArrayInputStream bais= new ByteArrayInputStream(IOUtils.toByteArray(inputStream));
            //Scanner scanner = new Scanner(bais); 
            Scanner scanner = new Scanner(inputStream); 
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
             
            
            
            //return Integer.toString(lineCount);
            return Integer.toString(countLine);
            
		}
		catch(AzureException e) {
			logErrMsg(e.getLocalizedMessage());
			return "-1";
		}
//		catch (IOException e) {
//        	logErrMsg(e.getLocalizedMessage());
//            return "-1";
//        } 
	}

	

	@Override
	public String getBucketName() {
		return this.containerName;
	}
	
	// will get the specific file in the container and then will use in another function
	// to get its properties like Size and lastModifiedDate
	
	public BlobContainerClient listCustomBucketObject(String bucket, String bucketPrefix) {
		
		String bucketWithPrefix = removeLastChar(bucket).concat("/").concat(bucketPrefix);
		BlobContainerClient ContainerClient= blobServiceClient.getBlobContainerClient(bucketWithPrefix);
		
		//BlobClient BlobClient = ContainerClient.getBlobClient(fileName);
		//BlobProperties BlobClientProperties = sourceBlobClient.getProperties();
//		
		
//		PagedIterable<BlobItem> blobItem = ContainerClient.listBlobs();
//		Iterator<BlobItem> list = (Iterator<BlobItem>) blobItem.iterator();
		
		return ContainerClient;
		
	}




	@Override
	public Integer moveFileWithinCloud(String sourceBucket, String sourceBucketPrefix, String targetBucket , String targetBucketPrefix, String fileName) {
	
//		String returnTargetBucketName = removeLastChar(targetBucket);
//		logMsg("returnTargetBucketName = "+ returnTargetBucketName);
//		String[] targetBucketSplitArr = returnTargetBucketName.split("/");
//		String targetBucketName = targetBucketSplitArr[0];
		
		if( sourceBucket.equals(targetBucket) && sourceBucketPrefix.equals(targetBucketPrefix) ) {
	    	 logMsg("Landing and Loading bucket are same. No Prefix added or Prefixes are equal.");
	    	 return 11;
	    }
	
		String sourceFileName = fileName;
	    String targetFileName = fileName;
	    
	    String sourceBucket1 = removeLastChar(sourceBucket).concat("/").concat(sourceBucketPrefix);
	    String targetBucket1 = removeLastChar(targetBucket).concat("/").concat(targetBucketPrefix);
	    
		logMsg("sourceBucket = "+ sourceBucket1);
		logMsg("targetBucket = "+ targetBucket1);
//	    if (!sourceBucket.equals(targetBucketName)) { //copy in target bucket
//	    	targetFileName = fileName;
//	    	
//	    }
//	    else { //copy in src bucket subfolder
//	    	targetFileName = fileName;   
//	    }
	    
	    
	    BlobContainerClient sourceContainerClient = blobServiceClient.getBlobContainerClient(sourceBucket1);
	    BlobContainerClient destContainerClient= blobServiceClient.getBlobContainerClient(targetBucket1);
	    
	    
	    BlobClient sourceBlobClient=sourceContainerClient.getBlobClient(sourceFileName);
	    BlobClient destBlobClient=destContainerClient.getBlobClient(targetFileName);
	    
	    //BlobProperties destBlobClient1 = destBlobClient.getProperties();
	  
	    
	    try {
			if (sourceBlobClient.exists() && !destBlobClient.exists()) {
				destBlobClient.beginCopy(sourceBlobClient.getBlobUrl(),null);
			}
	    }
	    
	    catch(Exception e){
	    	failedCopyFiles.add(fileName);
    		logMsg("Error: Failed to copy file: "+fileName+" to cloud bucket: "+targetBucket1);
	    	logErrMsg(e.getLocalizedMessage());
	    	return 11;
	    	
	    }
		
	    try {
	    	if(sourceBlobClient.exists() && destBlobClient.exists()) {
				sourceBlobClient.delete();
				if(!sourceBlobClient.exists() && destBlobClient.exists() ) {
					logMsg("File removed successfully from cloud source bucket.");
				}
				else {
					failedCopyFiles.add(fileName);
					logMsg("Error: Failed to remove file: "+fileName+" from cloud bucket: "+sourceBucket1);
					return 11;
				}
			}
			else {
				failedCopyFiles.add(fileName);
				logMsg("Either File Does not Exist in Source or Files did not copy to Destination");
				return 11;
			}
	    }
		catch(Exception e) {
			failedCopyFiles.add(fileName);
    		logMsg("Error: Failed to remove file: "+fileName+" from cloud bucket: "+sourceBucket1);
	    	logErrMsg(e.getLocalizedMessage());
	    	return 11;
		}
		
	   return 0;
	}
	
	
	
	// sourceBucket -> https://gcfrdatalake.blob.core.windows.net/gcfrbucket/
	// sourceBucketPrefix -> landingB
	// profile not being used 
public responseDTO moveBulkFilesWithinCloud(String sourceBucket, String sourceBucketPrefix, String targetBucket, String targetBucketPrefix, String profile, String fileName) {
		
		String fromBucketPrefix = "";
        String toBucket = "";
        
        String sourceURL = removeLastChar(sourceBucket);
        //logMsg("TBD Source URL "+ sourceURL);
        
        String storageAccountName = sourceURL.substring(8, sourceURL.indexOf('.', 0));
        //logMsg("TBD StorageAccountName "+ storageAccountName);
        
        sourceBucket = extractBucketName(sourceBucket);
        targetBucket = extractBucketName(targetBucket);
        
        sourceBucketPrefix = utils.standardizePrefixName(sourceBucketPrefix);
        targetBucketPrefix = utils.standardizePrefixName(targetBucketPrefix);
        
        if(!sourceBucketPrefix.isEmpty()) {
        	sourceBucketPrefix = utils.standardizePrefixName(sourceBucketPrefix);
        	fromBucketPrefix = sourceBucketPrefix + "/";
        } else {
        	fromBucketPrefix = "";
        }
        
        if(!targetBucketPrefix.isEmpty()) {
        	targetBucketPrefix = utils.standardizePrefixName(targetBucketPrefix);
        	toBucket = targetBucket + "/" + targetBucketPrefix;
        } else {
       	 toBucket = targetBucket;
        }
        
        //sourceBucketPrefix = "";
        //fileName = "CDR_2011-07-25_020000.000000.ctl";
		
        /*
         * Sample Command:
         * aws s3 mv s3://gcfrlanding s3://gcfrlanding/landing --exclude "*" --include "CDR_2011-07-25_*.ctl" --recursive
         */
        
        //fileName="CDR_2011-07-25_010000.000000.ctl";
		//String command = "azcopy copy "+fromBucket+" "+toBucket+" --exclude-pattern \"*\" --include-pattern \""+fileName+"\" --recursive";
		
//		String command = "az storage blob move -c gcfrbucket -d CDR_2011-07-25_120000.000000.ctl -s gcfrbucket/landingB/CDR_2011-07-25_120000.000000.ctl --account-name gcfrtesting";
//		logMsg("command: "+command);
        
        String originalFileName = fileName;
        
        
        // replace * -> (.*) because regex takes the second one 
        fileName = fileName.replace("*", "(.*)");
        //logMsg("TBD FILE NAME "+ fileName);
		
        //logMsg("TBD HEREEE");
        
        // get the blob client
        // TBD remove the bucket prefix 
        BlobContainerClient ContainerClient = listCustomBucketObject(sourceBucket, "");      

		PagedIterable<BlobItem> blobItem = ContainerClient.listBlobs();
		java.util.Iterator<BlobItem> list1 = blobItem.iterator();
		
		String patternString;
		ArrayList<String> ctlFile = new ArrayList<String>();
		
		if(!sourceBucketPrefix.isEmpty())
			patternString = "(^)"+sourceBucketPrefix+"(/)"+fileName+"";
		else 
			patternString = "(^)"+fileName+"";
			
		//logMsg("TBD patternString "+ patternString);
	    Pattern pattern = Pattern.compile(patternString, Pattern.CASE_INSENSITIVE);
	    
	    responseDTO login = authenticateAzureCLI();
	    if(login.getReturnCode()!=0) {
	    	logMsg("Could not Authenticate");
			login.setReturnCode(1);
			return login;
		}
	    
	    //logMsg("TBD Response Login "+ login);
	    
	    responseDTO logout = null;

		Matcher matcher;
		boolean matches;
		String item;
		String extractFileName;
		
		while(list1.hasNext()) {
			item = list1.next().getName();
			matcher = pattern.matcher(item);
			matches = matcher.matches();
			//logMsg("TBD MATCHES "+ matches);
			if(matches) {
				extractFileName= item.substring(item.lastIndexOf("/") + 1 ); // landingB/CDR_2011 -> CDR_2011 (will get the fullFileName only 
				//logMsg("TBD Extract "+ extractFileName);
				ctlFile.add(extractFileName);
				//logMsg("TBD File Name "+ item);
			}
			//else 
				//logMsg("TBD File Name "+ item);
		}
        
		String command = null;
		responseDTO res = null;
		
		//for(int i = 0; i < ctlFile.size(); i++) {   
		    //logMsg("TBD CTLFILE "+ctlFile.get(i)); 
		//}
		
		int i = 0;
				
		for(i = 0; i < ctlFile.size(); i++) {   
		    //logMsg(ctlFile.get(i)); 
		
			
		command = "az storage blob copy start --account-name "+storageAccountName+" --auth-mode login --destination-blob "+ctlFile.get(i)+" --destination-container "+toBucket+" --source-container "+sourceBucket+" --source-blob "+fromBucketPrefix+""+ctlFile.get(i)+"";
		logMsg("command: "+command);
		
		    try {
				res = utils.executeScript(command);
				//logMsg("TBD Loop "+ i);
				//logMsg("TBD Return Code "+ res.getReturnCode());
				if(res.getReturnCode()!=0) {
					logout = logoutAzureCLI();
					return res;
				}
			} catch (IOException | InterruptedException e) {
				e.printStackTrace();
				responseDTO r = new responseDTO();
				r.setReturnCode(1);
				r.setReturnMessage("Exception:"+e.getMessage());
				logout = logoutAzureCLI();
				return r;
			}
		}
		
		logMsg("Number of Files Copied: "+ i);
		
		String remove_command = "az storage blob delete-batch -s "+sourceBucket+" --auth-mode login --account-name "+storageAccountName+" --pattern "+fromBucketPrefix+""+originalFileName+"" ;
		logMsg(remove_command);
		
		try {
			responseDTO res_rm = utils.executeScript(remove_command);
			//logMsg("TBD Return Code "+ res.getReturnCode());
			if(res.getReturnCode()!=0) {
				logout = logoutAzureCLI();
				return res_rm;
			}
		} catch (IOException | InterruptedException e) {
			e.printStackTrace();
			responseDTO r = new responseDTO();
			r.setReturnCode(1);
			r.setReturnMessage("Exception:"+e.getMessage());
			logout = logoutAzureCLI();
			return r;
		}
		
		//logMsg("TBD Near Clear ");
		ctlFile.clear();
		//res.setReturnCode(0);
		//res.setReturnMessage("Files Moved Successfully");
		logout = logoutAzureCLI();
		return res;
		
//        try {
//			return utils.executeScript(command);
//		} catch (IOException | InterruptedException e) {
//			responseDTO r = new responseDTO();
//			r.setReturnCode(1);
//			r.setReturnMessage("Exception:"+e.getMessage());
//			
//			return r;
//		}		
	}



	@Override
	public responseDTO uploadFileslocalToCloudUsingCommand(String sourceDir, String targetBucket,
			String targetBucketPrefix, String fileName, boolean pathWithFile, String targetFileName) {
		
		String fromDir = "";
        String fileNameWithPrefix = "";
       
        if(pathWithFile == false) {
       	 	if(!sourceDir.isEmpty()) {
       	 		fromDir = sourceDir + "/" + fileName;
       	 	} 
       	 	else {
       	 		fromDir = sourceDir;
       	 	} 
        }
        else if (pathWithFile == true) {
       	 	fromDir = sourceDir; //sourceDir has fullpath with file
        }
        
        
        if(targetBucketPrefix.equals("")) {
       	 	fileNameWithPrefix = fileName;
        }
        else {
       	 	fileNameWithPrefix = targetBucketPrefix.concat("/").concat(fileName);
        }
        
        
        String targetBucketPrefixStandardize = removeLastChar(targetBucketPrefix);
        targetBucketPrefixStandardize = targetBucketPrefixStandardize.concat("/");
        
        String targetBucketStandardize = removeLastChar(targetBucket).concat("/").concat(targetBucketPrefixStandardize);
        
        BlobContainerClient targetContainerClient = blobServiceClient.getBlobContainerClient(targetBucketStandardize);
        
	    //File fileToBeUploaded = new File(fromDir);
	    try {
        
		    BlobClient targetBlobClient=targetContainerClient.getBlobClient(fileName);
		    
		    // will over write the file
		    targetBlobClient.uploadFromFile(fromDir,true);
		    
		    responseDTO r = new responseDTO();
			r.setReturnCode(0);
			r.setReturnMessage("File"+ fileName+" archived.");
	
	        return r;
	    }
	    catch (Exception e) {
	    	logMsg("Failed to Move File"+ fileName);
	    }
	    
	    responseDTO r = new responseDTO();
			r.setReturnCode(0);
			r.setReturnMessage("File"+ fileName+" archived.");

	        return r;
	}



	@Override
	public boolean checkCustomFileExists(String fileName, String bucket, String bucketPrefix, Integer type) {
		
		try {	
			
			String bucketWithPrefix = removeLastChar(bucket).concat("/").concat(bucketPrefix);
			BlobContainerClient ContainerClient= blobServiceClient.getBlobContainerClient(bucketWithPrefix);
			BlobClient BlobClient=ContainerClient.getBlobClient(fileName);
			return BlobClient.exists();
		}
		catch(Exception e){
			logMsg("Azure Blob does not exist or storage access issue "+ e.getLocalizedMessage());
			return false;
		}
	}



	@Override
	public Integer renameAndmoveWithinCloud(String sourceBucket, String sourceBucketPrefix, String targetBucket,
			String targetBucketPrefix, String fileName, String targetfilename) {

		if( sourceBucket.equals(targetBucket) && sourceBucketPrefix.equals(targetBucketPrefix) ) {
	    	 logMsg("Landing and Loading bucket are same. No Prefix added or Prefixes are equal.");
	    	 return 11;
	    }
		
		
		String sourceFileName = fileName;
	    String targetFileName = targetfilename;
	    
	    logMsg("SourceFileName "+ sourceFileName);
	    logMsg("TargetFileName "+ targetFileName);
	    
	    String sourceBucket1 = removeLastChar(sourceBucket).concat("/").concat(sourceBucketPrefix);
	    String targetBucket1 = removeLastChar(targetBucket).concat("/").concat(targetBucketPrefix);
	    
		logMsg("sourceBucket = "+ sourceBucket1);
		logMsg("targetBucket = "+ targetBucket1);
//	    if (!sourceBucket.equals(targetBucketName)) { //copy in target bucket
//	    	targetFileName = fileName;
//	    	
//	    }
//	    else { //copy in src bucket subfolder
//	    	targetFileName = fileName;   
//	    }
	    
	    
	    BlobContainerClient sourceContainerClient = blobServiceClient.getBlobContainerClient(sourceBucket1);
	    BlobContainerClient targetContainerClient= blobServiceClient.getBlobContainerClient(targetBucket1);
	    
	    
	    BlobClient sourceBlobClient=sourceContainerClient.getBlobClient(sourceFileName);
	    BlobClient targetBlobClient=targetContainerClient.getBlobClient(targetFileName);
	        
	    
	    try {
			if (sourceBlobClient.exists() && !targetBlobClient.exists()) {
				targetBlobClient.beginCopy(sourceBlobClient.getBlobUrl(),null);
			}
	    }
	    
	    catch(Exception e){
	    	failedCopyFiles.add(fileName);
    		logMsg("Error: Failed to copy file: "+fileName+" to cloud bucket: "+targetBucket1);
	    	logErrMsg(e.getLocalizedMessage());
	    	return 11;
	    	
	    }
	    
	    
	    try {
	    	
	    	BlobProperties sourceObjectProperty = sourceBlobClient.getProperties();
	    	BlobProperties targetObjectProperty = targetBlobClient.getProperties();
	    	
	    	logMsg("sourceObject.size(): "+sourceObjectProperty.getBlobSize());
	        logMsg("targetObject.size(): "+targetObjectProperty.getBlobSize());
	        
	     // needed to cast these long to Long so that they could be matched
	        Long sourceObjectSize = sourceObjectProperty.getBlobSize();
	        Long targetObjectSize = targetObjectProperty.getBlobSize();
	   
	        // needed to cast these Long to long so that they could be matched
	        
	        if(targetBlobClient!=null) {
	        	if( !sourceObjectSize.equals(targetObjectSize) ) {
	        		failedCopyFiles.add(fileName);
	        		logMsg("Error: File \"+fileName+\" copied to cloud bucket: "+targetfilename + " with exception their sizes mismatched.");
		    		return 11;
		        }
	        	
	        	if( sourceObjectProperty.getLastModified().isAfter(targetObjectProperty.getLastModified()) ) {
	        		failedCopyFiles.add(fileName);
	        		logMsg("Error: File "+fileName+" copied to cloud bucket: "+targetfilename + " with exception source file lastmodified is later then copied target file.");
		    		return 11;
	        		
	        	}
	        }
	        else {
	    		failedCopyFiles.add(fileName);
	    		logMsg("Error: Failed to copy file: "+fileName+" to cloud bucket: "+targetfilename);
	    		return 11;
	    	}
	    	
	        
	        if(sourceBlobClient.exists() && targetBlobClient.exists()) {
				sourceBlobClient.delete();
	        }
	        
	        if(sourceBlobClient!=null || sourceBlobClient.exists()) {
	        	logMsg("Warning: "+fileName+ " file may not have been deleted from "+ sourceBucket+ " but it has been copied to loading.");
	        }
	        else {
	        	logMsg("File "+fileName+" copied successfully to target bucket.");
	        }
	        	
	        return 0;
	        
			
		} catch (Exception e) {
			failedCopyFiles.add(fileName);
	    	logMsg("Error: Failed to copy file: "+fileName+" to cloud bucket: "+targetBucket);
	    	logErrMsg("Exception: "+e.getLocalizedMessage());
	        return 11;
		}
	    
	}


//
//	public responseDTO uploadFileslocalToCloudUsingCommand(String sourceDir, String targetBucket,
//			String targetBucketPrefix, String fileName,boolean pathWithFile,String targetFileName) {
//		// TODO Auto-generated method stub
//		return null;
//	}

	


	private String removeLastChar(String s)   
    {  
    	//returns the string after removing the last character  
		if(s.endsWith("/"))
			return s.substring(0, s.length() - 1); 
		else
			return s;
    }

	
	
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
	
	 public String extractBucketName(String bucketUrl) {
	    	String bucketName = removeLastChar(bucketUrl);
			bucketName = bucketName.substring(bucketName.lastIndexOf("/") + 1);
			
			return bucketName;
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
	
	public String getExtensionByApacheCommonLib(String filename) {
	    return FilenameUtils.getExtension(filename);
}



	@Override
	public String splitBucketUrl(String bucketUrl, String tempStartStr) {
		// TODO Auto-generated method stub
		return null;
	}







	@Override
	public String returnBucketName(String bucketUrl) {
		// TODO Auto-generated method stub
		 String returnLandingDirectory = removeLastChar(bucketUrl);
				 
	     return returnLandingDirectory.substring(returnLandingDirectory.lastIndexOf("/") + 1); //gcfr
	}

	public String returnBucketEndPointURL(String bucketUrl) {
		String returnLandingDirectory = removeLastChar(bucketUrl);
		 
	    return returnLandingDirectory.substring(0,returnLandingDirectory.lastIndexOf("/") + 1); //gcfr
	}

//
//	public responseDTO uploadFileslocalToCloudUsingCommand(String sourceDir, String targetBucket,
//			String targetBucketPrefix, String fileName,boolean pathWithFile,String targetFileName) {
//		// TODO Auto-generated method stub
//		return null;
//	}


	
}
