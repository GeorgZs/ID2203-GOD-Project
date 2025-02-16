#!/usr/bin/env bash

# Initial steps:
#  - Create GCP project "my-project-1499979282244"
#  - Enable service usage API
#  - Get gcloud CLI and run `gcloud auth login`

println_green() {
    printf "\033[0;32m$1\033[0m\n"
}

project_id="my-project-1499979282244"
deployment_service_account_id=deployment
deployment_service_account_mail="${deployment_service_account_id}@${project_id}.iam.gserviceaccount.com"

println_green "Setting up GCP project for"
echo "project_id: ${project_id}"

println_green "Enabling APIs"
gcloud services enable \
  artifactregistry.googleapis.com \
  compute.googleapis.com \
  iam.googleapis.com \
  storage.googleapis.com \
  cloudresourcemanager.googleapis.com \
  dns.googleapis.com
  # secretmanager.googleapis.com \

println_green "Creating deployment service account with id '${deployment_service_account_id}'"
gcloud iam service-accounts create "${deployment_service_account_id}" \
  --description="Used for the deployment application" \
  --display-name="Deployment Account"

println_green "Adding roles for service account"  
roles="artifactregistry.createOnPushRepoAdmin storage.admin compute.admin dns.admin iam.serviceAccountUser iam.serviceAccountTokenCreator iap.tunnelResourceAccessor"

for role in $roles; do
  gcloud projects add-iam-policy-binding "${project_id}" --member=serviceAccount:"${deployment_service_account_mail}" "--role=roles/${role}"
done;

# println_green "Creating firewall rules"
# gcloud compute firewall-rules create enable-ssh \
#     --allow tcp:22 \
#     --target-tags=enable-ssh \
#     --description="Allow SSH access for tagged instances"

println_green "Creating private DNS zone for VPC"
gcloud dns managed-zones create internal-network \
        --dns-name=internal.zone. \
        --visibility=private \
        --description="Private DNS zone for VPC" \
        --networks=default




gcloud projects add-iam-policy-binding my-project-1499979282244 \
    --member="user:<ADD USER HERE>" \
    --role="projects/my-project-1499979282244/roles/masters_project_admin_role"


gcloud iam roles create masters_project_admin_role \
    --project=my-project-1499979282244 \
    --title="MastersProjectAdminRole" \
    --description="Custom role for Master's students using GCP Python client API" \
    --permissions="artifactregistry.repositories.create,\
artifactregistry.repositories.get,\
artifactregistry.repositories.delete,\
artifactregistry.repositories.list,\
artifactregistry.repositories.update,\
compute.instances.create,\
compute.instances.delete,\
compute.instances.get,\
compute.instances.list,\
compute.instances.osAdminLogin,\
compute.instances.start,\
compute.instances.stop,\
compute.instances.update,\
dns.changes.create,\
dns.changes.get,\
dns.changes.list,\
dns.managedZones.create,\
dns.managedZones.delete,\
dns.managedZones.get,\
dns.managedZones.list,\
dns.managedZones.update,\
iam.serviceAccounts.actAs,\
iam.serviceAccounts.getAccessToken,\
storage.buckets.create,\
storage.buckets.delete,\
storage.buckets.get,\
storage.buckets.list,\
storage.objects.create,\
storage.objects.delete,\
storage.objects.get,\
storage.objects.list,\
iap.tunnelInstances.accessViaIAP"
