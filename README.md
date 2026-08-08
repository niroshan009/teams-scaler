# 🚀 Teams Scaler

Lightweight spring-boot project to listen to kafka topics and submit spark jobs to ths Kubernetes cluster.
Upon the spark job completion status, teams-scaler will publish a message to kafka with file name.

Intend to run team-scaler alongside the KEDA ScaledJob to reduce resource consumption.

## ✨ Features
- lightweight
- k8s resource optimized
- event driven
- Fault-Tolerant


## 📦 Getting Started

### 📋 Prerequisites
- JDK 17
- Maven
- Docker
- Orbstack with K8S cluster enabled (can replace with minikube for local cluster, but make sure to update yaml file with proper endpoint accordingly)
- HELM [Installation guide](https://helm.sh/docs/intro/install/)
- KEDA [Installation guid](https://keda.sh/docs/2.20/deploy/)

>### 💡 Note
> This project has a dependency with [ignite-spark](https://github.com/niroshan009/ignite-spark) which will process the data in spark. Not necessary to build locally 

### 🔨 Building

#### building artifacts
First project with below command to build artifact and copy the resource folder

```shell
mvn clean package
```

#### building docker image
Build docker image with the artifacts built in the previous step

```shell
docker build -t local/teams-scaler:latest .
```

## 🚀 Usage
This project intended to use with KEDA scaler. But you can deploy the application by running `kubectl apply -f deployment.yaml` command

### Local deployment
Run below commands:
* `docker compose up --force-recreate -d` : start below services
  * Iceberg : For data storage 
  * Iceberg-Rest : Iceberg catalog management
  * Kafka : For events
  * Rustfs : For data storing with iceberg and file handling
  * Ignite3 (2 nodes): to store reference data
* `kubectl create namespace spark` : create a spark namespace to run application and create spak jobs
* `kubectl apply -f spark-service-account.yaml` : create service account to run teams-scaler pods and submit spark jobs to spark namespace
* `kubectl apply -f load-scaled-job.yaml,enrich-scaled-job.yaml` : create KEDA scaled jobs to handle inbound file upload, new snapshot create events

#### Setting up Ignite3
To enrichment to work, we need to set up Ignite3, run below command
##### Setting up Ignite reference data

```shell
docker exec -it teams-scaler-node1-1 sh
/opt/ignite3cli/bin/ignite3 connect http://localhost:10300
cluster init --name=ignite3

sql

CREATE ZONE IF NOT EXISTS TeamsRefReplicated WITH replicas=3, partitions=25, storage_profiles='default';

CREATE TABLE Teamsref_v3(
    id UUID  DEFAULT rand_uuid(),
    orgId VARCHAR(200),
    orgName VARCHAR(200),
    teamId VARCHAR(200),
    teamName VARCHAR(200),
    projectId VARCHAR(200),
    projectName VARCHAR(200),
    deptId VARCHAR(200),
    deptName VARCHAR(200),
    officeId VARCHAR(200),
    loc VARCHAR(200),
    PRIMARY KEY(id)
) zone TeamsRefReplicated;

CREATE INDEX IF NOT EXISTS idx_orgId ON Teamsref_v3 (orgId);
CREATE INDEX IF NOT EXISTS idx_teamId ON Teamsref_v3 (teamId);
CREATE INDEX IF NOT EXISTS idx_projectId ON Teamsref_v3 (projectId);
```
Then insert reference data with stored in the `src/main/resources/sql/` directory

Then initialize the Ignite node 2.
```shell
docker exec -it teams-scaler-node2-1 sh
/opt/ignite3cli/bin/ignite3 connect http://localhost:10300
cluster init --name=ignite3
```

#### Creating topics (optional)
Below topics should have been created when you run `docker compose up --force-recreate -d`
* create below 4 topics in the kafka to handle events
  * `rustfs.inbound.upload.topic` : receives file upload events from Rustfs
  * `inbound.spark.job.failure` : publish events upon file processing success
  * `inbound.spark.job.success` : publish events upon file processing failure
  * `iceberg.raw.created.topic` : receives raw data created events from Rustfs

