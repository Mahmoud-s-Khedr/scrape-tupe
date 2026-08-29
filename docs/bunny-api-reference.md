# Bunny.net API Reference

> Research notes for the scrape-tupe project. Covers Magic Containers and Storage APIs.

---

## 1. Magic Containers REST API

### Base URL & Authentication
- **Base URL:** `https://api.bunny.net/mc`
- **Auth Header:** `AccessKey: <ACCOUNT_API_KEY>`
- **Content Headers:** `Content-Type: application/json`, `Accept: application/json`

### Core Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET    | `/mc/apps` | List all Magic Container applications |
| POST   | `/mc/apps` | Create a new container application |
| GET    | `/mc/apps/{appId}` | Get full app config and status |
| PUT    | `/mc/apps/{appId}` | Update app config (containers, env vars, scaling, regions) |
| DELETE | `/mc/apps/{appId}` | Delete an application |
| POST   | `/mc/apps/{appId}/deploy` | Deploy / rollout containers |
| POST   | `/mc/apps/{appId}/undeploy` | Stop all running instances |
| POST   | `/mc/apps/{appId}/restart` | Rolling restart of all pods |
| GET    | `/mc/apps/{appId}/overview` | Runtime health, active instances, CPU/RAM, latency |
| GET    | `/mc/apps/{appId}/autoscaling` | Autoscaling settings |

---

## 2. Autoscaling

- RL-based autoscaler keyed on CPU utilization.
- Configured **per-region** with `MinReplicas` and `MaxReplicas`.
- For our use case: set `min=N, max=N` to force exact replica count.
- **Trial Tier:** Max 3 instances per region, `MinReplicas * baseRegions <= 3`.

---

## 3. Instance Inspection

### `GET /mc/apps/{appId}/overview`
Returns:
- `ActiveInstances` — currently running pods across regions
- CPU usage, memory usage, latency

### `GET /mc/apps/{appId}`
Returns desired config including:
- `MinReplicas`, `MaxReplicas`
- Pod states per region: `Running`, `Pending`, `Terminating`

---

## 4. Bunny Storage API

### Base URLs (regional)
| Region | Endpoint |
|--------|----------|
| Default (EU) | `https://storage.bunnycdn.com` |
| London | `https://uk.storage.bunnycdn.com` |
| New York | `https://ny.storage.bunnycdn.com` |
| Los Angeles | `https://la.storage.bunnycdn.com` |
| Singapore | `https://sg.storage.bunnycdn.com` |
| Sydney | `https://syd.storage.bunnycdn.com` |
| São Paulo | `https://br.storage.bunnycdn.com` |
| Stockholm | `https://se.storage.bunnycdn.com` |

### Upload
```
PUT https://{region}.storage.bunnycdn.com/{storageZoneName}/{path}/{fileName}
Headers:
  AccessKey: <STORAGE_ZONE_PASSWORD>    # NOT the account API key!
  Content-Type: application/octet-stream
  Checksum: <SHA256_HEX>               # optional integrity check
Body: raw binary
```

### Download / Delete / List
```
GET    /{storageZoneName}/{path}/{fileName}     # download
DELETE /{storageZoneName}/{path}/{fileName}     # delete
GET    /{storageZoneName}/{path}/               # list files
```

---

## 5. Authentication Summary

| Service | Header | Key Type |
|---------|--------|----------|
| Magic Containers API | `AccessKey: <ACCOUNT_API_KEY>` | Account API Key |
| Storage API | `AccessKey: <STORAGE_ZONE_PASSWORD>` | Storage Zone Password |
