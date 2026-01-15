# 🎯 Spark UI Access Guide

## ⚠️ Important: Spark Streaming UI Behavior

**Spark Streaming UI chỉ xuất hiện khi có DATA đang được xử lý!**

Unlike batch jobs, Spark Streaming UI:
- ✅ Starts when data flows through
- ❌ May not appear if no data in Kafka
- ⏱️ Takes 10-30 seconds after first data

---

## 🚀 Quick Fix: Send Test Data

### Option 1: Run Scraper (Best)

```bash
# Terminal 1: Keep streaming-etl logs open
docker-compose logs -f streaming-etl

# Terminal 2: Run scraper
docker-compose up scraper

# Wait 30 seconds, then:
# Open: http://localhost:4040
```

### Option 2: Send Manual Test Message

```python
# test_streaming.py
from kafka import KafkaProducer
import json

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Send test match
test_match = {
    "match_id": 9999999,
    "season": "2025-26",
    "matchweek": 99,
    "match_info": {
        "home_team": "Test Home",
        "away_team": "Test Away",
        "home_score": 1,
        "away_score": 1
    }
}

producer.send('raw-match-data', value=test_match)
producer.flush()
print("✅ Test message sent!")
```

Then run:
```bash
python test_streaming.py
```

---

## 📊 What to Expect

### Timeline:
```
T+0s:   Send data to Kafka
T+5s:   Streaming query picks up data
T+10s:  Spark UI appears at :4040
T+15s:  Processing batch visible
T+30s:  Batch completed, metrics updated
```

### In Browser (http://localhost:4040):

**When NO data:**
- Connection refused or empty page

**When data flowing:**
- ✅ Spark logo
- ✅ **Streaming** tab visible
- ✅ Active query shown
- ✅ Input rate graph
- ✅ Completed batches list

---

## 🔍 Verify Streaming is Working

### Check 1: Logs show query running
```bash
docker-compose logs streaming-etl | grep "Streaming query started"

# Should see: ✅ Streaming query started!
```

### Check 2: Kafka has data
```
http://localhost:8080
→ Topics → raw-match-data
→ Should have messages
```

### Check 3: Container healthy
```bash
docker-compose ps streaming-etl

# Should show: Up (healthy)
```

---

## 💡 Why UI Doesn't Appear?

**Common reasons:**

1. **No data in Kafka** (most common)
   - Streaming query is idle
   - UI doesn't initialize until first batch
   
2. **Spark UI not started yet**
   - Wait 60 seconds after container start
   
3. **Port conflict**  
   - Check: `netstat -ano | findstr :4040`
   - If occupied, change port in docker-compose.yml

4. **Container restarting**
   - Check logs for errors
   - May need to fix config issues

---

## ✅ Recommended Test Flow

```bash
# 1. Ensure streaming is running
docker-compose ps streaming-etl
# Status: Up

# 2. Check logs
docker-compose logs --tail=10 streaming-etl
# Should see: "Streaming query started!"

# 3. Send data!
docker-compose up scraper
# OR
python test_streaming.py

# 4. Wait 30 seconds

# 5. Open browser
http://localhost:4040

# 6. Should see Spark Streaming UI! ✅
```

---

## 🎯 Success Indicators

When working:

1. ✅ **Logs**: "Streaming query started"
2. ✅  **Container**: Status "Up"
3. ✅ **Data**: Messages in Kafka topic
4. ✅ **Processing**: Logs show "Batch X completed"
5. ✅ **UI**: http://localhost:4040 shows Streaming tab

---

## 🆘 Still Not Working?

### Try alternative port

Edit `docker-compose.yml`:
```yaml
streaming-etl:
  ports:
    - "4042:4040"  # Use 4042 instead
```

Then:
```bash
docker-compose up -d streaming-etl
http://localhost:4042  # Try new port
```

### Enable Spark UI debug logs

Add to `streaming_etl.py`:
```python
spark.sparkContext.setLogLevel("INFO")  # More verbose
```

---

## 📝 Key Takeaway

**Spark Streaming UI = Lazy Loading**

- Won't appear until data flows
- Need active processing
- Just run scraper to trigger!

---

**Run scraper now to see Spark UI appear!** 🚀

```bash
docker-compose up scraper
```

Then refresh: http://localhost:4040 (after 30s)
