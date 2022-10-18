# Aws_Azure_GCP_Functions

Using Java SDKs for AWS, Azure and GCP to get authenticate, list bucket content and perform other different operations. This was done to allow the user
to have data in cloud storages and using sdk, cli listing, getting content.


### Table of Contents

- [Overview](#overview)
- [AWS](#aws)
- [Azure](#azure)
- [GCP](#gcp)
- [Program Flow](#program-flow)
- [Maven](#maven)
- [Documentation and Material](#documentation-and-material)
- [Tools and Technologies](#tools-and-technologies)

---


### Overview

- Java Cloud Interface was implemented by Aws, Azure and GCP code so that have a common api calling. 
- All 3 have same function name that interface makes us to implement but their own way of executing.
- This was code is being used in many different patterns Reg, Load, Bulk Reg, Bulk Load, Export, NOS Read, NOS Write.
- This code is just a small part of our something bigger. 

---

### AWS

- Will need to install the AWS CLI and configure our credentials.
- As this all was coded in java and spring boot was used will also need to add in maven dependencies.
- Check the maven site for aws cli ones.

```bash
  aws configure
```
- The above will prompt us to enter our AccessId, AccessKey, Region, Output Format.
- After this has been configured we will have an .aws folder and will have 2 files
  - credentials
  - config
  
- Our code will use these files to authenticate itself so that it can perform the different functions.

<p align="center">
  <img src="Images/aws.JPG" >
</p>

---

#### Azure

- Azure will need to install some utilities
  - az copy
  - az 

- Pretty easy to download these and step up.
- If you are going to use a terminal to list content of bucket you will need to provide credentials for both az copy and az.
- Login to your azure portal and create a service principal, grant blob storage access to your user.
- Once you are done will that you will need to copy ClientID/AppID, Tenant/TenantId, Secret/ClientSecret.
- Create a **credentials** file on your root user path, this path will be provided to our code.

```bash
  ClientId=
  Tenantd=
  ClientSecret=
  StorageAccountName=
```

```bash
set AZCOPY_SPA_CLIENT_SECRET=<ClientSecret>

azcopy login --service-principal  --application-id <ClientID> --tenant-id=<TenantID>
```

- Similarly if you are going to use az.

```bash
az login --service-principal -u <ClientId> --tenant <TenantId> -p <ClientSecret>
```

-- To check if az copy and az working try these commands.

```bash
  azcopy list https://<StorageAccount>.blob.core.windows.net/<bucket>/
```

```bash
  az storage fs file list -f <bucket> --account-name <StorageAccount> --auth-mode login
```

- After setting this all we will be able to execute the functions in azure.

---

### GCP

- GCP will require us to install the sdk.
- It can be downloaded for windows and linux. After downloading and unzipping will configure it.

```bash
  ./google-cloud-sdk/install.sh
```

```bash
  ./google-cloud-sdk/bin/gcloud init
```

- The steps are in the documentation follow those. It will ask for account and other details.


<p align="center">
  <img src="Images/google.JPG">
</p>

---

### Program Flow

- All the 3 cloud pattern implement the cloud interface. This allow us to have same function name but different implemention.
- The 3 cloud patterns are used in a number of different processes based on user cloud vendor they can use which one to use.
- 1 for AWS, 2 for GCP and 3 for Azure.

<p align="center">
  <img src="Images/Inheritance.jpg" width="700">
</p>




---

### Maven 

```bash
	  <!-- AWS -->
		
      	<dependency>
      		<groupId>software.amazon.awssdk</groupId>
      		<artifactId>s3</artifactId>
    	</dependency>
			
		<!-- Azure From VSC -->
		
		<dependency>
		      <groupId>com.azure</groupId>
		      <artifactId>azure-identity</artifactId>
		      <version>1.2.0</version>
	    </dependency>
	    
	    <dependency>
	        <groupId>com.azure</groupId>
	        <artifactId>azure-storage-blob</artifactId>
	        <version>12.8.0</version>
	    </dependency>
		
		<dependency>
		    <groupId>com.azure</groupId>
		    <artifactId>azure-core</artifactId>
		    <version>1.18.0</version>
		</dependency>
				
		<!-- Needed to get this dependency for it to work else was not working --> 
		<dependency>
			<groupId>com.microsoft.azure</groupId>
			<artifactId>msal4j</artifactId>
			<version>1.9.1</version> 
		</dependency> 
		
		
    	<dependency>
      		<groupId>com.google.cloud</groupId>
      		<artifactId>google-cloud-storage</artifactId>
    	</dependency>
```

---

### Documentation and Material

- [Azure Service Principal](https://learn.microsoft.com/en-us/azure/active-directory/develop/howto-create-service-principal-portal)
- [AWS Configure](https://docs.aws.amazon.com/cli/latest/reference/configure/)
- [GCP Configure](https://cloud.google.com/sdk/docs/initializing)

  
---
### Tools and Technologies

- Java Spring Boot
- AWS, Azure, GCP SDK, CLI
- Maven

