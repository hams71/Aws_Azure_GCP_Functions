# Aws_Azure_GCP_Functions

Using Java SDKs for AWS, Azure and GCP to get authenticate, list bucket content and perform other different operations. This was done to allow the user
to have data in cloud storages.


### Table of Contents

- [Overview](#overview)
- [Data Build Tool](#data-build-tool)
- [Folder Structure](#folder-structure)
- [Program Flow](#program-flow)
- [Program Execution](#program-execution)
- [Data Model](#data-model)
- [Level Up](#level-up)
- [Documentation and Material](#documentation-and-material)
- [Tools and Technologies](#tools-and-technologies)

---


### Overview

- Java Cloud Interface was implemented by Aws, Azure and GCP code so that have a common api calling. 
- All 3 have same function name that interface makes us to implement but their own way of executing.
- This was code is being used in many different patterns Reg, Load, Bulk Reg, Bulk Load, Export, NOS Read, NOS Write.


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


### Folder Structure
- dbt &emsp;&emsp;&emsp; - dbt cloned repo used for installation    
- dbt-evn &emsp;&nbsp;- python virtual env related
- dbt-model 
  - dbt-model &emsp; - after dbt init <name> this is created
    - analyses
    - macros &emsp;&emsp;&emsp;&emsp;&emsp; - create macros here and refer later
    - models &emsp;&emsp;&emsp;&emsp;&emsp; - tables, views, incremental load, merge 
    - seeds &emsp;&emsp;&emsp;&nbsp;&nbsp;&emsp;&emsp; - flat files incase want to load to staging tables using dbt
    - snapshots &emsp;&nbsp;&nbsp;&emsp;&emsp; - SCD tables
    - tests &emsp;&emsp;&emsp;&emsp;&emsp;&emsp; - tests on different models
    - dbt_project.yml &emsp;&nbsp; - one place to configure all
    - packages.yml &emsp;&emsp; - dbt has many packages which can be downloaded


---

### Program Flow

<p align="center">
  <img src="Images/Flow.jpg" width="850" >
</p>




---

### Program Execution
  
- Before executing any of the commands remember to be in the correct folder.
```bash
  cd <project-name>
```
  
- To load file from seeds folder to Stage Tables in snowflake.
```bash
  dbt seed
```
  
- The data will be in the Stage Tables, now will load data to Core/Dim tables.
  - City, Country, Transations will be loaded as they have no history handling needed.
```bash
  dbt run
```
- To run a specific folder inside model folder.
```bash
  dbt run -m <folder-name>
```

- The Snapshot folder has all those models on which SCD-2 is being used.
```bash
  dbt snapshot
```
  
- We can also check test cases that are defined on different models, snapshots, seeds.
```bash
  dbt test
```
- dbt provides a web UI that can be accessed using.
  - Internally it has all metadata in json that is saved and used by the web UI
```bash
  dbt docs generate
  dbt docs serve
```
  
- You can check different things in the UI and also the lineage as well.
  
<p align="center">
  <img src="Images/dbt-lineage.JPG", width="800">
</p>


---

 

### Documentation and Material

- [Azure Service Principal](https://learn.microsoft.com/en-us/azure/active-directory/develop/howto-create-service-principal-portal)
- [AWS Configure](https://docs.aws.amazon.com/cli/latest/reference/configure/)
- [dbt Youtube Playlist](https://www.youtube.com/playlist?list=PLy4OcwImJzBLJzLYxpxaPUmCWp8j1esvT)
- [Snowflake Youtube Playlist](https://www.youtube.com/playlist?list=PLy4OcwImJzBIX77cmNYiXIJ3tBhpNSUKI)
- Thanks to Kahan Data Solutions for the demo videos.
  
---
### Tools and Technologies

- Dbt
- Snowflake
- Git

