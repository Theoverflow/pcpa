# Service Delivery Handbook: Hybrid Multi-Cloud Tableau Analytics Platform

This handbook provides detailed step-by-step procedures for engineering and operations teams implementing the hybrid multi-cloud analytics solution. It covers infrastructure provisioning, networking, security, microservices deployment, data pipelines, and monitoring.

---

## Table of Contents

1. Prerequisites & Environment Setup
2. Networking & Connectivity
3. Kubernetes Bastion Hosts
4. API Gateway & Service Mesh
5. Transversal Microservices (Kafka, Chef, Redis)
6. Containerizing & Deploying Tableau
7. Data Ingestion Pipelines
8. Cloud Data Lake & Query Engine
9. CI/CD & GitOps Automation
10. Observability, Logging & Backup
11. Operational Runbooks

---

## 1. Prerequisites & Environment Setup

1. **Accounts & Permissions**
   - Ensure AWS, Azure, and GCP accounts with:
     - Admin/API access for Terraform
     - IAM roles/service principals preconfigured
   - On-prem credentials for MySQL and Vertica database admin
2. **Tooling**
   - Install:
     - Terraform v1.4+
     - kubectl v1.26+
     - Helm v3.9+
     - Docker CLI v24+
     - AWS CLI, Azure CLI, gcloud SDK
     - Git, Vault CLI
3. **Code Repositories**
   - Git repositories for:
     - Infrastructure-as-Code (Terraform modules)
     - Kubernetes manifests & Helm charts
     - Data-pipeline configs (NiFi, Debezium)
   - Branching strategy: `main`, `dev`, `feature/*`
4. **Secret Management**
   - Vault cluster with KV v2 mounted at `secret/data/hybrid-platform`
   - Store DB credentials, cloud API tokens, TLS certs.

---

## 2. Networking & Connectivity

### 2.1 On-Prem to Cloud VPN

- **AWS VPN Setup** (similar for Azure ExpressRoute, GCP Interconnect):

  1. In AWS Console, create Customer Gateway with on-prem public IP.
  2. Create Virtual Private Gateway and attach to your VPC.
  3. Create Site-to-Site VPN connection, download configuration.
  4. Apply config to on-prem firewall/router.
  5. Verify tunnel UP: `aws ec2 describe-vpn-connections --filters Name=state,Values=available`

- **Routing**:

  - On-prem: route MySQL (10.10.0.0/16) and Vertica (10.20.0.0/16) via VPN.
  - Cloud VPC route table: add 0.0.0.0/0 via IGW, and on-prem CIDRs via VGW.

### 2.2 Security Groups & Network Policies

- **Cloud Security Groups**:
  - Allow inbound only from bastion nodes on SSH (22), HTTPS (443).
  - Allow K8s ingress on 80/443 for Tableau only.
- **Kubernetes Network Policies**:
  - Deny all ingress/egress by default.
  - Allow Tableau namespace to reach DB IPs on ports 3306 (MySQL) and 5433 (Vertica).

---

## 3. Kubernetes Bastion Hosts

1. **Provision Minimal Clusters**
   - Terraform modules to create EKS, AKS, GKE with:
     - Node-pool `bastion`: 1-2 t3.small or B2s nodes.
2. **Deploy SSH DaemonSet**:
   - Create `bastion` namespace.
   - DaemonSet manifest (sshd) mounting SSH keys from Vault via CSI driver.
3. **Access Flow**:
   - Engineers SSH into bastion via cloud LB.
   - From bastion pod: `kubectl exec -it tableau-pod -- bash`.

---

## 4. API Gateway & Service Mesh

### 4.1 Kong Gateway

1. **Helm Install**:
   ```bash
   helm repo add kong https://charts.konghq.com
   helm install kong kong/kong \
     --namespace kong \
     --set ingressController.installCRDs=false \
     --set env.database=off \
     --set env.proxy_listen=0.0.0.0:8000,0.0.0.0:8443 ssl
   ```
2. **IngressRoute** manifests to route `/api/*` to microservices.

### 4.2 Envoy Service Mesh

1. **Istio/Envoy Deployment** via Helm or operator.
2. **mTLS**: enable strict mutual TLS in peerAuthentication.
3. **VirtualServices** to route traffic between services.

---

## 5. Transversal Microservices

### 5.1 Kafka (Strimzi)

1. **Operator Install**:
   ```bash
   kubectl create namespace kafka
   helm repo add strimzi https://strimzi.io/charts/
   helm install strimzi strimzi/strimzi-kafka-operator -n kafka
   ```
2. **KafkaCluster CR** with 3 brokers, Zookeeper nodes.
3. **Topics**:
   - `db-cdc-mysql`, `db-batch-vertica`, `tableau-refresh`

### 5.2 Chef Server

1. **Containerized Chef Server**:
   - Use official `chef/server` image.
   - PVC for `/var/opt/chef-server` and `/etc/chef-server`.
2. **Access** via Kong at `chef.company.com`.
3. **Automate Node Bootstrapping** with chef-client pods.

### 5.3 Redis Caching

1. **Helm Install**:
   ```bash
   helm repo add bitnami https://charts.bitnami.com/bitnami
   helm install redis bitnami/redis \
     --namespace cache \
     --set architecture=standalone,auth.enabled=true
   ```
2. **Usage**:
   - Cache API tokens, session data, query results.

---

## 6. Containerizing & Deploying Tableau

1. **Dockerfile** (simplified):
   ```dockerfile
   FROM ubuntu:22.04
   RUN apt-get update && \
       apt-get install -y wget tcsh xvfb libxss1 libnss3
   ADD tableau-server-2023.2.0.deb /tmp/
   RUN dpkg -i /tmp/tableau-server-*.deb
   # Install MySQL & Vertica drivers
   COPY drivers/* /opt/tableau/drivers/
   ENTRYPOINT ["/opt/tableau/tableau_server/scripts/start-server.sh"]
   ```
2. **Helm Chart**:
   - `values.yaml`:
     ```yaml
     replicaCount: 3
     env:
       TABLEAU_AUTH: "${TABLEAU_TOKEN}"
       DB_MYSQL_HOST: "10.10.0.5"
       DB_VERTICA_HOST: "10.20.0.8"
     persistence:
       enabled: true
       storageClass: gp2
       size: 200Gi
     ```
3. **Deploy**:
   ```bash
   helm install tableau ./charts/tableau -n analytics
   ```
4. **Verify**:
   - `kubectl get pods -n analytics` (should show 3 running)
   - Access UI via `kubectl port-forward svc/tableau 8443:443`

---

## 7. Data Ingestion Pipelines

### 7.1 Debezium + Kafka Connect (CDC)

1. **Connect Cluster**:
   - Deploy Kafka Connect pod with Debezium MySQL connector plugin.
   - Mount credentials via Vault.
2. **Connector Config** (`mysql-source.json`):
   ```json
   { "name": "mysql-source", "config": {
     "connector.class": "io.debezium.connector.mysql.MySqlConnector",
     "database.hostname": "10.10.0.5",
     "database.user": "dbuser",
     "database.password": "${vault:secret/data/hybrid-platform#mysql-password}",
     "database.server.id": "184054",
     "table.include.list": "sales.*,hr.*",
     "topic.prefix": "db-cdc-"
   }}
   ```
3. **Deploy**:
   ```bash
   kubectl apply -f mysql-source.json -n kafka
   ```

### 7.2 Apache NiFi (Batch)

1. **Helm Install** NiFi:
   ```bash
   helm repo add cetic https://cetic.github.io/helm-nifi
   helm install nifi cetic/nifi --namespace ingest
   ```
2. **NAR Packages** for Vertica used in NiFi.
3. **Flow**:
   - `GetFile` → `ConvertAvroToORC` → `PutHDFS` (to S3/ADLS/GCS)

---

## 8. Cloud Data Lake & Query Engine

1. **Object Storage**:
   - AWS S3 buckets with versioning & encryption.
   - Azure Data Lake Gen2 containers.
   - GCS buckets.
2. **Catalog**:
   - Glue Data Catalog / Azure Purview / GCP Data Catalog.
   - Use AWS Glue Crawler pointing at `s3://hybrid-platform/raw/`
3. **Query**:
   - Athena: Create external tables on Parquet partitions.
   - Synapse: Serverless SQL pools.
   - BigQuery: Create external tables via federated GCS.

---

## 9. CI/CD & GitOps Automation

1. **Argo CD**:
   - Install in `gitops` namespace.
   - Create `Application` YAMLs pointing to Git repos:
     ```yaml
     apiVersion: argoproj.io/v1alpha1
     kind: Application
     metadata: { name: tableau-app }
     spec:
       project: default
       source: { repoURL: "git@github.com:org/hybrid-platform", path: "charts/tableau" }
       destination: { server: "https://kubernetes.default.svc", namespace: analytics }
     ```
2. **Terraform Cloud/Enterprise**:
   - Workspace per cloud.
   - Use remote state in S3/Key Vault/GCS.
   - Run plans on PRs.

---

## 10. Observability, Logging & Backup

1. **Prometheus & Grafana**:
   - Helm install in `monitoring` namespace.
   - Scrape targets: Kong, Kafka, Tableau, NiFi.
2. **ELK/EFK**:
   - Filebeat DaemonSet shipping logs to Elasticsearch.
3. **Backup**:
   - Velero for K8s snapshots to object store.
   - RDS snapshots and Vertica exports scheduled via cronjobs.

---

## 11. Operational Runbooks

- **Incident Response**: Steps to triage VPN down, cluster unreachable, Tableau service failure.
- **Scaling**: How to add nodes, adjust HPA for Tableau.
- **Upgrades**: Rolling upgrade procedure for Helm releases.
- **DR Tests**: Quarterly drills for failover between clouds.

---

*End of Service Delivery Handbook*

