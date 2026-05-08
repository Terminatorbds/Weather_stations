# Weather Stations — Run Commands

## SCENARIO 1: Cold start (after a full shutdown / fresh boot)

# 1. Make sure Docker Desktop is running (open from Start menu — has whale icon).
#    Wait until the whale icon stops animating. Then in PowerShell:

# 2. Start Minikube
minikube start --memory=4096 --cpus=4 --driver=docker

# 3. Make sure all 4 images are loaded into Minikube
minikube image load weather-station:1.0
minikube image load kafka-processor:1.0
minikube image load central-station:1.0

# 4. Apply all manifests (idempotent — safe to run even if some objects exist)
kubectl apply -f k8s/

# 5. Wait until all pods are Running (Ctrl+C the watch when settled)
kubectl get pods -w

# 6. Apply the database schema
$pgpod = (kubectl get pods -l app=postgres -o jsonpath='{.items[0].metadata.name}')
Get-Content sql\schema.sql | kubectl exec -i $pgpod -- psql -U weather_user -d weather_db

# 7. Verify the table exists
kubectl exec -i $pgpod -- psql -U weather_user -d weather_db -c "\d weather_readings"


## SCENARIO 2: Warm start (Minikube was stopped, images still loaded)

minikube start
kubectl get pods -w
# Ctrl+C when all 14 are Running


## SCENARIO 3: Demo commands (during presentation)

# --- Open Kafka UI ---
# Run in its own dedicated PowerShell window. Keep it open the whole demo.
kubectl port-forward svc/kafka-ui-svc 8080:8080
# Then open in browser: http://localhost:8080

# --- Open Kubernetes Dashboard ---
# Run in another dedicated PowerShell window. It auto-opens the browser.
minikube dashboard

# --- Tail the central station (great visual for "5000-row batch" moment) ---
kubectl logs -f deployment/central-station

# --- Tail the rain processor (constant rain alerts firing) ---
kubectl logs -f deployment/kafka-processor

# --- Tail one weather station ---
kubectl logs -f weather-station-0

# --- Show all pods ---
kubectl get pods


## SCENARIO 4: Run the analytical SQL queries (the spec's deliverable)

$pgpod = (kubectl get pods -l app=postgres -o jsonpath='{.items[0].metadata.name}')

# Total rows so far
kubectl exec -i $pgpod -- psql -U weather_user -d weather_db -c "SELECT COUNT(*) FROM weather_readings;"

# Distinct station IDs (sanity check — should show 1..10)
kubectl exec -i $pgpod -- psql -U weather_user -d weather_db -c "SELECT DISTINCT station_id FROM weather_readings ORDER BY station_id;"

# Query 1: Battery distribution per station
Get-Content sql\query1_battery.sql | kubectl exec -i $pgpod -- psql -U weather_user -d weather_db

# Query 2: Estimated dropped messages per station
Get-Content sql\query2_drops.sql | kubectl exec -i $pgpod -- psql -U weather_user -d weather_db


## SCENARIO 5: Reset to clean slate (if you want to re-demo from zero)

# Stop the central station (so it doesn't re-insert old data mid-truncate)
kubectl scale deployment/central-station --replicas=0

# Truncate the database
$pgpod = (kubectl get pods -l app=postgres -o jsonpath='{.items[0].metadata.name}')
kubectl exec -i $pgpod -- psql -U weather_user -d weather_db -c "TRUNCATE weather_readings RESTART IDENTITY;"

# Skip backlog: reset central station's consumer offsets to "now"
$kafkapod = (kubectl get pods -l app=kafka -o jsonpath='{.items[0].metadata.name}')
kubectl exec -it $kafkapod -- kafka-consumer-groups --bootstrap-server localhost:9092 --group central-station --topic weather-readings --reset-offsets --to-latest --execute

# Bring central station back up
kubectl scale deployment/central-station --replicas=1


## SCENARIO 6: Full shutdown (when you're done)

# Stop Minikube (preserves all state — next start is fast)
minikube stop

# Or, NUKE EVERYTHING (use only if you want to reset to factory zero):
# minikube delete


## QUICK DIAGNOSTICS (if something looks off)

kubectl get pods                                    # snapshot
kubectl describe pod <pod-name>                     # detailed status
kubectl logs <pod-name> --previous                  # logs from a crashed pod
kubectl rollout restart statefulset/weather-station # bounce all stations
kubectl rollout restart deployment/central-station  # bounce central
kubectl rollout restart deployment/kafka-processor  # bounce processor