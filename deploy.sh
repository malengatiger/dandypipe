echo "🌀🌀🌀Deploying DataDriver 🌀🌀🌀"
echo "🌀🌀🌀Creating container and pushing it to GCP registry 🌀🌀🌀"

./mvnw compile com.google.cloud.tools:jib-maven-plugin:3.3.1:build \
  -Dimage=gcr.io/thermal-effort-366015/datadriver

echo "🍎🍎🍎Deploy newly created container to Cloud Run 🍎🍎🍎"
gcloud run deploy datadriver \
     --region=europe-west1 \
     --platform=managed \
     --project=thermal-effort-366015 \
     --allow-unauthenticated \
     --update-env-vars "GOOGLE_CLOUD_PROJECT=thermal-effort-366015" \
     --image=gcr.io/thermal-effort-366015/datadriver

echo "🍎🍎🍎 Deployed DataDriver Cloud Run 🍎🍎🍎"