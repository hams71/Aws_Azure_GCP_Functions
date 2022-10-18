
public interface GCFR_Cloud_Storage_Int {
	
	//Pass cloud credentials, build cloud object, also checks if bucket exists
	public Integer aunthenticateIntoCloud();
	
	//return object(Hashmap) which contains filename(key) with extension(value)
	public Object listBucketObjects();
	
	//should return true if the file exists
	//type: 0 -> print error
	//type: 1 -> dont print error
	public boolean checkBucketFileExists(String fileName, String bucketPrefix, Integer type);
	
	public boolean checkCustomFileExists(String fileName,String bucket, String bucketPrefix, Integer type);
	
	//For CTL File content, should return a single string
	public String getBucketFileContent(String fileName, String bucketPrefix);
	
	//Check bucket exists
	public boolean checkBucketExists(String bucketName);
	
	//For Data file, to get count of lines
	public String getBucketFileLineCount(String fileName, String bucketPrefix);
	
	//Move file from source to target
	public Integer moveFileWithinCloud(String sourceBucket, String sourceBucketPrefix, String targetBucket,  String targetBucketPrefix, String fileName);
	
	public Integer renameAndmoveWithinCloud(String sourceBucket, String sourceBucketPrefix, String targetBucket,  String targetBucketPrefix, String sourcefilename,String targetfilename);

	//Move multiple files from source bucket to target bucket using CLI command
	public responseDTO moveBulkFilesWithinCloud(String sourceBucket, String sourceBucketPrefix, String targetBucket , String targetBucketPrefix,String profile, String fileName); 

	//Upload file from local to Cloud using SDK
	public responseDTO uploadFileslocalToCloudUsingCommand(String sourceDir, String targetBucket , String targetBucketPrefix,String fileName, boolean pathWithFile,String targetFileName);

	//get bucket name
	public String getBucketName();
	
	//split bucket by url
	public String splitBucketUrl(String bucketUrl, String tempStartStr);
	
	public String returnBucketName(String bucketUrl);		

}
