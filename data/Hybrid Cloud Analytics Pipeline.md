## Detailed Implementation Guide

This document provides detailed, step-by-step implementation instructions for setting up a multi-cloud hybrid analytics platform with:

- On-prem MySQL/Vertica cluster
- Kubernetes bastion hosts in AWS (EKS), Azure (AKS), and GCP (GKE)
- Containerized Tableau Server on Kubernetes
- Data ingestion pipelines (Apache NiFi & Debezium/Kafka)
- Cloud data lakes (S3, ADLS Gen2, GCS)
- SQL-on-lake query engines (Athena, Synapse, BigQuery)
- API Gateway & transversal microservices (Kong/Envoy, Kafka, Chef, Redis)
- GitOps-driven CI/CD
- Monitoring, logging, and backup

---

### 1. Prerequisites

1. **On-Prem Environment:**

   - MySQL v8+ with binlog enabled (for CDC)
   - Vertica v10+ accessible via JDBC
   - Network firewall rules allowing outbound to cloud VPN endpoints

2. **Cloud Accounts:**

   - AWS, Azure, GCP accounts with permissions to create VPCs/VNets, K8s clusters, IAM roles, storage

3. **Tools Installed Locally:**

   - Terraform v1.4+
   - kubectl v1.25+
   - Helm v3.8+
   - AWS CLI, Azure CLI, gcloud SDK
   - Git

---

### 2. Networking & VPN Setup

#### 2.1 AWS Site-to-Site VPN

```hcl
# terraform/aws/vpn.tf
provider "aws" {
  region = var.aws_region
}

resource "aws_vpc" "main" {
  cidr_block = "10.0.0.0/16"
  tags = { Name = "hybrid-vpc" }
}

resource "aws_customer_gateway" "onprem" {
  bgp_asn    = 65000
  ip_address = var.onprem_static_ip
  type       = "ipsec.1"
}

resource "aws_vpn_gateway" "vgw" {
  vpc_id = aws_vpc.main.id
  tags   = { Name = "vgw" }
}

resource "aws_vpn_connection" "vpn" {
  customer_gateway_id = aws_customer_gateway.onprem.id
  vpn_gateway_id      = aws_vpn_gateway.vgw.id
  type                = "ipsec.1"
  static_routes_only  = true
}

resource "aws_vpn_connection_route" "route" {
  count                = length(var.onprem_cidrs)
  vpn_connection_id    = aws_vpn_connection.vpn.id
  destination_cidr_block = var.onprem_cidrs[count.index]
}
```

#### 2.2 Azure & GCP Connectivity

- Use `azurerm_virtual_network_gateway`, `azurerm_local_network_gateway`, and `azurerm_virtual_network_gateway_connection` for Azure.
- Use `google_compute_vpn_gateway`, `google_compute_vpn_tunnel` in Terraform for GCP.

---

### 3. Kubernetes Cluster Provisioning

#### 3.1 AWS EKS with Terraform

```hcl
# terraform/aws/eks.tf
module "eks" {
  source          = "terraform-aws-modules/eks/aws"
  cluster_name    = "hybrid-eks"
  cluster_version = "1.25"
  subnets         = aws_subnet.private.*.id

  node_groups = {
    bastion = {
      desired_capacity = 1
      instance_type    = "t3.small"
      labels = { role = "bastion" }
      ssh_key_name = var.ssh_key
    }
    worker = {
      desired_capacity = 2
      instance_type    = "t3.medium"
      labels = { role = "app" }
    }
  }
}
```

#### 3.2 Azure AKS

```hcl
# terraform/azure/aks.tf
resource "azurerm_kubernetes_cluster" "aks" {
  name                = "hybrid-aks"
  location            = var.azure_region
  resource_group_name = azurerm_resource_group.rg.name
  dns_prefix          = "hybrid"

  default_node_pool {
    name       = "nodepool"
    node_count = 3
    vm_size    = "Standard_DS2_v2"
  }

  identity {
    type = "SystemAssigned"
  }
}
```

#### 3.3 GCP GKE

```hcl
# terraform/gcp/gke.tf
resource "google_container_cluster" "gke" {
  name               = "hybrid-gke"
  location           = var.gcp_region
  initial_node_count = 3

  network    = google_compute_network.vpc.self_link
  subnetwork = google_compute_subnetwork.subnet.self_link

  node_config {
    machine_type = "e2-medium"
    oauth_scopes = ["https://www.googleapis.com/auth/cloud-platform"]
  }
}
```

---

### 4. Bastion Setup & SSH Access

1. Label bastion nodes: `kubectl label nodes -l role=bastion bastion=true`.
2. Deploy SSH DaemonSet:

```yaml
apiVersion: apps/v1
kind: DaemonSet
metadata:
  name: ssh-bastion
  namespace: kube-system
spec:
  selector:
    matchLabels:
      app: ssh-bastion
  template:
    metadata:
      labels:
        app: ssh-bastion
    spec:
      nodeSelector:
        bastion: "true"
      containers:
      - name: sshd
        image: linuxserver/openssh-server
        env:
        - name: PUID
          value: "1000"
        - name: PGID
          value: "1000"
        - name: TZ
          value: "UTC"
        - name: PUBLIC_KEY
          valueFrom:
            secretKeyRef:
              name: ssh-keys
              key: public
        ports:
        - containerPort: 22
          hostPort: 22
      tolerations:
      - key: "node-role.kubernetes.io/master"
        operator: "Exists"
        effect: "NoSchedule"
```

3. Create `ssh-keys` secret with your public key.

---

### 5. API Gateway & Transversal Microservices

To provide unified ingress, routing, and shared services across clouds:

#### 5.1 API Gateway (Kong + Envoy)

1. **Deploy Kong Gateway** via Helm:
   ```bash
   helm repo add kong https://charts.konghq.com
   helm install kong-gateway kong/kong \
     --namespace kong-system --create-namespace \
     --set ingressController.installCRDs=true
   ```
2. **Expose Gateway** with a LoadBalancer service. Configure DNS for a common API endpoint per cloud.
3. **Envoy Sidecar** (optional) via Istio/Linkerd for mTLS and advanced routing.

#### 5.2 Kafka (Strimzi Operator)

1. **Install Strimzi**:
   ```bash
   kubectl create namespace kafka
   helm repo add strimzi https://strimzi.io/charts/
   helm install strimzi-kafka strimzi/strimzi-kafka-operator --namespace kafka
   ```
2. **Kafka Cluster CR**:
   ```yaml
   apiVersion: kafka.strimzi.io/v1beta2
   kind: Kafka
   metadata:
     name: hybrid-kafka
     namespace: kafka
   spec:
     kafka:
       replicas: 3
       listeners:
         - name: plain
           port: 9092
           type: internal
       storage:
         type: ephemeral
     zookeeper:
       replicas: 3
       storage:
         type: ephemeral
     entityOperator: {}
   ```

#### 5.3 Chef (Configuration Management)

1. **Deploy Chef Server** container:
   ```bash
   kubectl create namespace chef
   kubectl apply -f https://raw.githubusercontent.com/chef/chef-server/master/deploy/k8s/chef-server-deployment.yaml
   ```
2. **Bootstrap Nodes**: use `knife bootstrap` to register Kubernetes nodes and on-prem VMs.
3. **Cookbooks & Policies**: store in Git and automate via Chef Automate.

#### 5.4 Redis (Caching & Session Store)

1. **Install Redis** via Bitnami Helm chart:
   ```bash
   helm repo add bitnami https://charts.bitnami.com/bitnami
   helm install redis-cache bitnami/redis --namespace data-services
   ```
2. **Access**: expose internally via ClusterIP; restrict with NetworkPolicies.
3. **Use Cases**: caching query results, session tokens, leaderboards for dashboard metadata.

---

### 6. Containerizing & Deploying Tableau Server

*(next sections…)*

