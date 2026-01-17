---
layout: default
title: Load Testing
nav_order: 8
---

# Load Testing

Simulate football matches with 100K+ concurrent viewers to test system performance.

## Overview

The load testing tools simulate two types of events:

### Game Events (on the field)
- `pass`, `shot`, `goal`, `foul`, `yellow_card`, `red_card`
- `corner`, `free_kick`, `penalty`, `offside`, `substitution`
- `interception`, `tackle`, `save`, `var_review`

### Engagement Events (viewer reactions)
- **Reactions**: cheers, boos, emoji reactions
- **Comments**: match commentary, player discussions
- **Video Actions**: pause, rewind, replay, camera switch
- **Shares**: Twitter, Facebook, WhatsApp, in-app
- **Predictions**: score predictions, polls, player ratings
- **Clicks**: stats views, player profiles, merchandise

---

## Key Concept: Engagement Correlation

Viewer engagement correlates with game events. A goal triggers 15x more reactions than baseline activity:

| Game Event | Engagement Multiplier |
|------------|----------------------|
| Goal | 15x baseline |
| Own Goal | 12x baseline |
| Red Card | 10x baseline |
| Penalty | 8x baseline |
| VAR Review | 6x baseline |
| Shot on Target | 4x baseline |
| Yellow Card | 2.5x baseline |

---

## Viewer Personas

The simulation models different viewer behavior patterns:

| Persona | Distribution | Characteristics |
|---------|-------------|-----------------|
| Casual Viewer | 55% | Low engagement, occasional reactions |
| Active Fan | 25% | High engagement, reactions + comments |
| Social Sharer | 10% | Frequent sharing, captures moments |
| Stats Enthusiast | 7% | High stats clicks, predictions |
| Bettor | 3% | Very reactive to game-changing moments |

---

## Quick Start

### 1. Set Up Python Environment

```bash
cd tests/load
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### 2. Configure API

```bash
export API_URL="http://localhost:8080"
export API_KEY="your-api-key"
```

### 3. Run Simulation

```bash
# Quick test (1,000 viewers, 5 minutes)
python match_simulator.py --viewers 1000 --duration 5

# Full simulation (100K viewers, 90 minutes)
python match_simulator.py --viewers 100000 --duration 90
```

---

## Scripts

### `match_simulator.py` - Full Match Simulation

Simulates a complete match with game events and viewer engagement.

```bash
python match_simulator.py \
  --match-id "match_abc123" \
  --api-url "http://localhost:8080" \
  --api-key "your-api-key" \
  --viewers 100000 \
  --duration 90 \
  --batch-size 500 \
  --concurrency 50
```

**Options:**

| Option | Default | Description |
|--------|---------|-------------|
| `--match-id` | Auto-generated | Match identifier |
| `--api-url` | `http://localhost:8080` | API base URL |
| `--api-key` | From env `API_KEY` | API authentication key |
| `--viewers` | `100000` | Number of concurrent viewers |
| `--duration` | `90` | Match duration in minutes |
| `--batch-size` | `500` | Engagement events per batch |
| `--concurrency` | `50` | Concurrent HTTP requests |

### `simulate_match.py` - Game Events Only

Simulates only game events (no viewer engagement).

```bash
python simulate_match.py
```

### `viewer_simulator.py` - Engagement Only

Simulates only viewer engagement events.

```bash
python viewer_simulator.py \
  --match-id "match_123" \
  --viewers 100000 \
  --duration 90
```

---

## VS Code Tasks

Pre-configured tasks for load testing:

| Task | Description |
|------|-------------|
| Load Test Setup | Create Python virtual environment |
| Load Test (1K) | Quick test with 1,000 viewers |
| Load Test (100K) | Full test with 100,000 viewers |

Run via Command Palette: `Tasks: Run Task`

---

## Sample Output

```
============================================================
MATCH SIMULATION: AlHilal vs AlNassr
Match ID: match_8f3a2bc1
Target Viewers: 100,000
Duration: 90 minutes
============================================================

Ramping up to 100,000 viewers over 120s...
  Added 10,000/100,000 viewers
  Added 20,000/100,000 viewers
  ...
Ramp-up complete: 100,000 viewers active

Min  0 | Score: 0-0 | Viewers: 100,000 | Engagements: 12,453
Min  5 | Score: 0-0 | Viewers: 99,234 | Engagements: 8,123
...
Min 23 | Score: 0-0 | Viewers: 98,456 | Engagements: 9,876
Minute 23: GOAL - Score: 1-0
Min 23 | Score: 1-0 | Viewers: 98,456 | Engagements: 145,234  <-- SPIKE!
...
Min 90 | Score: 2-1 | Viewers: 87,654 | Engagements: 125,000

======================================================================
MATCH SIMULATION COMPLETE
======================================================================

FINAL SCORE:         AlHilal 2 - 1 AlNassr
Match ID:            match_8f3a2bc1

----------------------------------------------------------------------
GAME EVENTS ON FIELD
----------------------------------------------------------------------
Total Events:        3,245

By Type:
  pass                 2,100
  shot                    89
  foul                    78
  goal                     2
  ...

----------------------------------------------------------------------
VIEWER ENGAGEMENT
----------------------------------------------------------------------
Peak Viewers:        100,000
Total Engagements:   2,456,789
Eng/Viewer:          24.6

Engagement by Type:
  reaction           1,523,456 (62.0%)
  comment              368,518 (15.0%)
  video_action         245,679 (10.0%)
  click                196,543 ( 8.0%)
  share                 73,593 ( 3.0%)
  prediction            49,000 ( 2.0%)

Peak Engagement: Minute 23 (145,234 engagements)

----------------------------------------------------------------------
API PERFORMANCE
----------------------------------------------------------------------
Total Calls:         5,234
Errors:                  12
Error Rate:           0.23%
Avg Latency:          45.2ms
======================================================================
```

---

## Engagement Event Types

### Reaction Subtypes
`cheer`, `boo`, `emoji_goal`, `emoji_fire`, `emoji_clap`, `emoji_cry`, `emoji_angry`, `emoji_heart`, `emoji_laugh`, `emoji_wow`

### Comment Subtypes
`match_commentary`, `player_discussion`, `team_support`, `trash_talk`, `question`

### Video Action Subtypes
`pause`, `play`, `rewind`, `replay`, `camera_switch`, `quality_change`, `fullscreen`

### Share Subtypes
`twitter`, `facebook`, `whatsapp`, `instagram`, `in_app`, `copy_link`

### Prediction Subtypes
`score_prediction`, `next_goal`, `player_rating`, `poll_vote`, `man_of_match`

### Click Subtypes
`stats_view`, `player_profile`, `team_info`, `lineup`, `ad_click`, `merchandise`, `ticket`

---

## Performance Tips

1. **Start Small**: Begin with `--viewers 1000` to verify setup
2. **Batch Size**: Use `--batch-size 500` for optimal throughput
3. **Concurrency**: Adjust `--concurrency 50` based on API capacity
4. **Gradual Ramp-up**: Viewers are added gradually to avoid overwhelming the API
5. **Monitor Grafana**: Watch dashboards during tests for bottlenecks

---

## Monitoring During Tests

### Grafana Dashboards

1. Open http://localhost:3005 (dev) or https://grafana.your-domain.com (prod)
2. Navigate to Dashboards
3. Watch for:
   - Request rate and latency
   - Kafka consumer lag
   - ClickHouse write throughput
   - System resources (CPU, memory)

### ClickHouse Queries

```sql
-- Events per minute
SELECT
  toStartOfMinute(timestamp) as minute,
  count() as events
FROM engagement_events
WHERE match_id = 'match_8f3a2bc1'
GROUP BY minute
ORDER BY minute;

-- Engagement by type
SELECT
  engagement_type,
  count() as cnt
FROM engagement_events
WHERE match_id = 'match_8f3a2bc1'
GROUP BY engagement_type;
```

---

## Team Data

The simulation uses player rosters from CSV files in `tests/load/`:
- `alhilal.csv` - Al Hilal player data
- `alnassr.csv` - Al Nassr player data

**CSV format:**
```csv
Name,Position,Number
Player Name,MF,10
```

---

## Dependencies

- `aiohttp>=3.9.0` - Async HTTP client
- `tqdm>=4.66.0` - Progress bar
- `requests>=2.31.0` - Sync HTTP client
- `python-dotenv>=1.0.0` - Environment variable loading
