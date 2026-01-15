**Check if services are running:**

```bash
# From devcontainer
curl -s http://clickhouse:8123/ping && echo "ClickHouse OK"
curl -s http://prometheus:9090/-/healthy && echo "Prometheus OK"
curl -s http://grafana:3000/api/health && echo "Grafana OK"
```
