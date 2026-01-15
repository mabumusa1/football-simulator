# Football Match & Engagement Load Testing

This directory contains load testing tools that simulate both **game events on the field** and **viewer engagement** for football matches. The key insight is that user engagement (reactions, comments, shares) is **correlated** with game events (goals, red cards, etc.).

## Overview

Two types of events are simulated:

### 1. Game Events (What happens on the field)
- `pass`, `shot`, `goal`, `foul`, `yellow_card`, `red_card`
- `corner`, `free_kick`, `penalty`, `offside`, `substitution`
- `interception`, `tackle`, `save`, `var_review`

### 2. Engagement Events (How viewers react)
- **Reactions**: cheers, boos, emoji reactions (⚽🔥👏😢😡)
- **Comments**: match commentary, player discussions, team support
- **Video Actions**: pause, rewind, replay, camera switch
- **Shares**: Twitter, Facebook, WhatsApp, in-app sharing
- **Predictions**: score predictions, polls, player ratings
- **Clicks**: stats views, player profiles, merchandise

## Key Concept: Engagement Correlation

Engagement events are **correlated** with game events:

| Game Event | Engagement Multiplier |
|------------|----------------------|
| Goal       | 15x baseline         |
| Own Goal   | 12x baseline         |
| Red Card   | 10x baseline         |
| Penalty    | 8x baseline          |
| VAR Review | 6x baseline          |
| Shot on Target | 4x baseline      |
| Yellow Card | 2.5x baseline       |

## Viewer Personas

The simulation models different viewer behavior patterns:

| Persona | Distribution | Characteristics |
|---------|-------------|-----------------|
| Casual Viewer | 55% | Low engagement, occasional reactions |
| Active Fan | 25% | High engagement, reactions + comments |
| Social Sharer | 10% | Frequent sharing, captures moments |
| Stats Enthusiast | 7% | High stats clicks, predictions |
| Bettor | 3% | Very reactive to game-changing moments |

## Scripts

### `match_simulator.py` - Combined Match & Engagement Simulator

Simulates a full match with both game events and 100K+ concurrent viewers generating engagement.

```bash
# Basic usage (100K viewers, 90-minute match)
python match_simulator.py

# Custom configuration
python match_simulator.py \
  --match-id "match_abc123" \
  --api-url "http://localhost:8080" \
  --api-key "your-api-key" \
  --viewers 100000 \
  --duration 90 \
  --batch-size 500 \
  --concurrency 50
```

**Output:**
- Game events sent to `/api/events`
- Engagement events batched to `/api/engagements`
- Real-time progress logging
- Comprehensive final statistics

### `simulate_match.py` - Game Events Only

Original simulator for game events only (no viewer engagement).

```bash
python simulate_match.py
```

### `viewer_simulator.py` - Engagement Events Only

Simulates viewer engagement with detailed persona modeling.

```bash
python viewer_simulator.py \
  --match-id "match_123" \
  --viewers 100000 \
  --duration 90
```

## Setup

### 1. Create Virtual Environment

```bash
cd tests/load
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### 2. Configure API

Set environment variables or use defaults:

```bash
export API_URL="http://localhost:8080"
export API_KEY="your-api-key"
export WEBHOOK_URL="http://localhost:8080/api/events"
```

### 3. Run Simulation

```bash
# Full simulation with engagement
python match_simulator.py --viewers 100000

# Quick test with fewer viewers
python match_simulator.py --viewers 1000 --duration 5
```

## API Endpoints

The simulator sends events to two endpoints:

### `POST /api/events` - Game Events

```json
{
  "eventId": "uuid",
  "matchId": "match_123",
  "eventType": "goal",
  "teamId": 1,
  "playerId": "player_7",
  "timestamp": "2026-01-15T10:30:00Z",
  "metadata": {
    "score_home": 1,
    "score_away": 0,
    "assist": "Player Name"
  }
}
```

### `POST /api/engagements` - Engagement Events (Batch)

```json
{
  "events": [
    {
      "event_id": "uuid",
      "match_id": "match_123",
      "user_id": "user_abc",
      "session_id": "sess_xyz",
      "engagement_type": "reaction",
      "engagement_subtype": "cheer",
      "related_game_event_id": "game_event_uuid",
      "game_minute": 45,
      "device_type": "mobile",
      "platform": "ios",
      "country_code": "SA",
      "content": "⚽🔥",
      "metadata": {"persona": "active_fan"},
      "timestamp": "2026-01-15T10:30:01Z"
    }
  ]
}
```

## Engagement Event Types

### Reaction Subtypes
- `cheer`, `boo`, `emoji_goal`, `emoji_fire`, `emoji_clap`
- `emoji_cry`, `emoji_angry`, `emoji_heart`, `emoji_laugh`, `emoji_wow`

### Comment Subtypes
- `match_commentary`, `player_discussion`, `team_support`
- `trash_talk`, `question`

### Video Action Subtypes
- `pause`, `play`, `rewind`, `replay`
- `camera_switch`, `quality_change`, `fullscreen`

### Share Subtypes
- `twitter`, `facebook`, `whatsapp`, `instagram`
- `in_app`, `copy_link`

### Prediction Subtypes
- `score_prediction`, `next_goal`, `player_rating`
- `poll_vote`, `man_of_match`

### Click Subtypes
- `stats_view`, `player_profile`, `team_info`
- `lineup`, `ad_click`, `merchandise`, `ticket`

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
Min 45 | Score: 1-0 | Viewers: 95,123 | Engagements: 35,678
Minute 45: HALF_TIME - Score: 1-0
...
Min 90 | Score: 2-1 | Viewers: 87,654 | Engagements: 125,000
Minute 90: FULL_TIME - Score: 2-1

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

## Dependencies

- `aiohttp>=3.9.0` - Async HTTP client for high-concurrency requests
- `tqdm>=4.66.0` - Progress bar
- `requests>=2.31.0` - Sync HTTP client
- `python-dotenv>=1.0.0` - Environment variable loading

## Performance Tips

1. **Batch Size**: Use `--batch-size 500` for optimal throughput
2. **Concurrency**: Adjust `--concurrency 50` based on API capacity
3. **Viewer Ramp-up**: Viewers are added gradually to avoid overwhelming the API
4. **Local Testing**: Start with `--viewers 1000` for quick tests

## Team Data

The simulation uses player rosters from CSV files:
- `alhilal.csv` - Al Hilal player data
- `alnassr.csv` - Al Nassr player data

CSV format:
```csv
Name,Position,Number
Player Name,MF,10
```
