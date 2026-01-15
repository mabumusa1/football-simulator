import csv
import random
import uuid
import os
import requests
from datetime import datetime, timedelta
from dotenv import load_dotenv
from tqdm import tqdm

# Load environment variables from .env file (looks in current dir and parent dirs)
load_dotenv()
load_dotenv(os.path.join(os.path.dirname(__file__), '..', '..', '.env'))

ALHILAL_CSV = "alhilal.csv"
ALNASSR_CSV = "alnassr.csv"

# Webhook URL for sending events (from environment variable)
WEBHOOK_URL = os.getenv("WEBHOOK_URL", "http://localhost:8080/api/events")

# Team ID constants
TEAM_ID_AL_HILAL = 2
TEAM_ID_AL_NASSR = 1

def load_players(csv_path):
    players = []
    with open(csv_path, newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            players.append({
                'id': row['id'],
                'name': row['name'],
                'position': row['position'].lower()
            })
    return players

def select_team(players):
    # Common football team setups (goalkeeper is always 1)
    formations = [
        {'def': 4, 'mid': 4, 'fwd': 2},  # 4-4-2
        {'def': 4, 'mid': 3, 'fwd': 3},  # 4-3-3
        {'def': 3, 'mid': 5, 'fwd': 2},  # 3-5-2
        {'def': 5, 'mid': 3, 'fwd': 2},  # 5-3-2
        {'def': 4, 'mid': 5, 'fwd': 1},  # 4-5-1
        {'def': 3, 'mid': 4, 'fwd': 3},  # 3-4-3
    ]
    formation = random.choice(formations)
    # Always 1 goalkeeper
    full_formation = {'gk': 1}
    full_formation.update(formation)
    selected = []
    for pos, count in full_formation.items():
        pos_players = [p for p in players if p['position'] == pos]
        if len(pos_players) < count:
            raise ValueError(f"Not enough players for position {pos}")
        selected += random.sample(pos_players, count)
    return selected, full_formation


def get_event_metadata(event_type, player, all_players, team_id, team1_id=TEAM_ID_AL_HILAL, team2_id=TEAM_ID_AL_NASSR):
    """Create extra information for different types of events."""
    metadata = {"action": event_type}

    # Different ways to handle each type of event
    event_handlers = {
        "pass": lambda: handle_pass(player, all_players, team_id),
        "shot": lambda: {"shooter_id": player['id']},
        "goal": lambda: handle_goal(player, all_players, team_id),
        "foul": lambda: handle_foul(player, all_players, team_id, team1_id, team2_id),
        "yellow_card": lambda: {"player_id": player['id'], "card_type": "yellow", "reason": "unsporting behavior"},
        "red_card": lambda: {"player_id": player['id'], "card_type": "red", "reason": "serious foul play"},
        "offside": lambda: {"player_id": player['id']},
        "corner": lambda: {"type": "corner", "taker_id": player['id']},
        "free_kick": lambda: {"type": "free_kick", "taker_id": player['id']},
        "interception": lambda: handle_interception(player, all_players, team_id, team1_id, team2_id)
    }

    handler = event_handlers.get(event_type)
    if handler:
        metadata.update(handler())

    return metadata

def handle_pass(player, all_players, team_id):
    """Add details about who passed to whom."""
    target = random.choice([p for p in all_players[team_id] if p['id'] != player['id']])
    return {"from_id": player['id'], "to_id": target['id']}

def handle_goal(player, all_players, team_id):
    """Add details about who scored and who helped."""
    metadata = {"scorer_id": player['id']}
    # 70% chance of having someone who passed the ball before the goal
    if random.random() < 0.7:
        assister = random.choice([p for p in all_players[team_id] if p['id'] != player['id']])
        metadata["assist_id"] = assister['id']
    return metadata

def handle_foul(player, all_players, team_id, team1_id, team2_id):
    """Add details about who did the foul and who was fouled."""
    opponent_team_id = team2_id if team_id == team1_id else team1_id
    victim = random.choice(all_players[opponent_team_id])
    return {"fouler_id": player['id'], "victim_id": victim['id']}

def handle_interception(player, all_players, team_id, team1_id, team2_id):
    """Add details about who took the ball and from whom."""
    opponent_team_id = team2_id if team_id == team1_id else team1_id
    intercepted_from = random.choice(all_players[opponent_team_id])
    return {"interceptor_id": player['id'], "intercepted_from_id": intercepted_from['id']}

def create_event(event_type, match_id, minute, team_id, player, all_players, current_time, team1_id=TEAM_ID_AL_HILAL, team2_id=TEAM_ID_AL_NASSR):
    """Create one event record with all the needed information."""
    event = {
        "eventId": str(uuid.uuid4()),
        "matchId": match_id,
        "eventType": event_type,
        "timestamp": (current_time + timedelta(minutes=minute-1)).isoformat() + "Z",
        "teamId": team_id,
        "playerId": player['id'],
        "metadata": get_event_metadata(event_type, player, all_players, team_id, team1_id, team2_id)
    }
    return event

def simulate_events(team1, team2, team1_id=TEAM_ID_AL_HILAL, team2_id=TEAM_ID_AL_NASSR, minutes=90):
    # Types of events that can happen and how often they occur per minute
    event_types = [
        ("pass", 0.65),        # Most common - players passing the ball
        ("shot", 0.12),        # When a player tries to score
        ("goal", 0.03),        # When a shot goes in the net
        ("foul", 0.08),        # Breaking the rules
        ("yellow_card", 0.02), # Warning for bad behavior
        ("red_card", 0.005),   # Serious punishment - player leaves the game
        ("substitution", 0.0), # No player changes in first half
        ("offside", 0.02),     # Being too far forward when receiving the ball
        ("corner", 0.03),      # When the ball goes out near the goal
        ("free_kick", 0.03),   # Free shot after a foul
        ("interception", 0.04), # Taking the ball from the other team
    ]
    # Make sure all chances add up to 1.0 (100%)
    total_prob = sum([p for _, p in event_types])
    event_types = [(e, p/total_prob) for e, p in event_types]
    match_id = str(uuid.uuid4())
    all_players = {team1_id: team1, team2_id: team2}
    # Start counting time from now
    current_time = datetime.now().replace(second=0, microsecond=0)
    print(f"\nSimulating {minutes} minutes of the match (Match ID: {match_id[:16]}...)")
    print(f"Sending events to webhook: {WEBHOOK_URL}")

    total_events = 0
    events_per_minute = []
    all_events = []
    sent_events = 0

    # Show progress as we go through each minute
    with tqdm(total=minutes, desc="Match Progress", unit="min", bar_format='{desc}: {percentage:3.0f}%|{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}]') as pbar:
        for minute in range(1, minutes+1):
            n_events = random.randint(20, 50)  # 20-50 things happen per minute (like a real match)
            events_per_minute.append(n_events)
            total_events += n_events

            for _ in range(n_events):
                event_type = random.choices([e for e, _ in event_types], [p for _, p in event_types])[0]
                team_id = random.choice([team1_id, team2_id])
                player = random.choice(all_players[team_id])
                event = create_event(event_type, match_id, minute, team_id, player, all_players, current_time, team1_id, team2_id)
                all_events.append(event)

                # Send event to webhook
                try:
                    response = requests.post(WEBHOOK_URL, json=event, timeout=5)
                    if response.status_code in (200, 202):  # 202 Accepted for async processing
                        sent_events += 1
                except Exception:
                    pass  # Ignore failures in test context

            # Update progress bar with current info
            pbar.set_postfix({
                'events': f'{n_events}',
                'total': f'{total_events}',
                'sent': f'{sent_events}'
            })
            pbar.update(1)

    print(f"\nSimulation complete! Total events: {total_events}")
    print(f"Events per minute: {events_per_minute}")
    print(f"Average events per minute: {total_events/minutes:.1f}")
    print(f"Events sent to webhook: {sent_events}")

    # Return all events for later use
    return all_events

def main():
    team1_players = load_players(ALHILAL_CSV)
    team2_players = load_players(ALNASSR_CSV)
    team1_eleven, team1_formation = select_team(team1_players)
    team2_eleven, team2_formation = select_team(team2_players)
    print("Al Hilal (Formation: GK-{gk}-{def}-{mid}-{fwd}):".format(**team1_formation))
    for p in team1_eleven:
        print(f"  {p['name']} ({p['position'].upper()})")
    print("\nAl Nassr (Formation: GK-{gk}-{def}-{mid}-{fwd}):".format(**team2_formation))
    for p in team2_eleven:
        print(f"  {p['name']} ({p['position'].upper()})")
    simulate_events(team1_eleven, team2_eleven, TEAM_ID_AL_HILAL, TEAM_ID_AL_NASSR, minutes=5)

if __name__ == "__main__":
    main()