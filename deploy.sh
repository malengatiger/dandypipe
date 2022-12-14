echo "πππDeploying DataDriver πππ"
echo "πππCreating container and pushing it to GCP registry πππ"

./mvnw compile com.google.cloud.tools:jib-maven-plugin:3.3.1:build \
  -Dimage=gcr.io/thermal-effort-366015/datadriver

echo "πππDeploy newly created container to Cloud Run πππ"
gcloud run deploy datadriver \
     --region=europe-west1 \
     --platform=managed \
     --project=thermal-effort-366015 \
     --allow-unauthenticated \
     --update-env-vars "GOOGLE_CLOUD_PROJECT=thermal-effort-366015" \
     --image=gcr.io/thermal-effort-366015/datadriver

echo "πππ Deployed DataDriver Cloud Run πππ"