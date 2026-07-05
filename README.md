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
* `docker compose up -d` : start below services
  * Iceberg : For data storage 
  * Iceberg-Rest : Iceberg catalog management
  * Kafka : For events
  * Rustfs : For data storing with iceberg and file handling
* `kubectl create namespace spark` : create a spark namespace to run application and create spak jobs
* `kubectl apply -f spark-service-account.yaml` : create service account to run teams-scaler pods and submit spark jobs to spark namespace
* `kubectl apply -f teams-scaled-job.yaml` : create KEDA scaled jobs to handle Kafka events
* create below 3 topics in the kafka to handle events
  * `rustfs.inbound.upload.topic` : receives file upload events from Rustfs
  * `inbound.spark.job.failure` : publish events upon file processing success
  * `inbound.spark.job.success` : publish events upon file processing failure

