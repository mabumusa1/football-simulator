#!/usr/bin/env python3
"""
Combined Match & Engagement Simulator

Generates BOTH:
1. Game Events - what happens on the field (passes, shots, goals, fouls)
2. Engagement Events - how 100K viewers react to those game events

The key insight: engagement spikes are CORRELATED to game events.
- Goal scored → massive reaction spike
- Red card → angry reactions + comments
- Normal play → baseline engagement

This simulates a realistic match where we can analyze HOW people engage.
"""

import asyncio
import aiohttp
import json
import random
import uuid
import time
import argparse
import logging
import csv
import os
from dataclasses import dataclass, field, asdict
from typing import List, Dict, Optional, Tuple
from datetime import datetime, timezone
from enum import Enum
from collections import defaultdict
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# =============================================================================
# Game Event Types (what happens on the field)
# =============================================================================

class GameEventType(Enum):
    KICKOFF = "kickoff"
    PASS = "pass"
    SHOT = "shot"
    SHOT_ON_TARGET = "shot_on_target"
    GOAL = "goal"
    OWN_GOAL = "own_goal"
    FOUL = "foul"
    YELLOW_CARD = "yellow_card"
    RED_CARD = "red_card"
    CORNER = "corner"
    FREE_KICK = "free_kick"
    PENALTY = "penalty"
    PENALTY_SAVED = "penalty_saved"
    OFFSIDE = "offside"
    SUBSTITUTION = "substitution"
    INTERCEPTION = "interception"
    TACKLE = "tackle"
    SAVE = "save"
    INJURY = "injury"
    VAR_REVIEW = "var_review"
    HALF_TIME = "half_time"
    SECOND_HALF = "second_half"
    FULL_TIME = "full_time"


# Probability weights for game events per minute (baseline)
GAME_EVENT_WEIGHTS = {
    GameEventType.PASS: 60.0,
    GameEventType.INTERCEPTION: 5.0,
    GameEventType.TACKLE: 4.0,
    GameEventType.FOUL: 3.0,
    GameEventType.SHOT: 2.5,
    GameEventType.SHOT_ON_TARGET: 1.5,
    GameEventType.CORNER: 1.5,
    GameEventType.FREE_KICK: 2.0,
    GameEventType.OFFSIDE: 1.0,
    GameEventType.SAVE: 1.0,
    GameEventType.GOAL: 0.15,  # ~3 goals per 90 min average
    GameEventType.YELLOW_CARD: 0.08,
    GameEventType.RED_CARD: 0.01,
    GameEventType.SUBSTITUTION: 0.1,
    GameEventType.INJURY: 0.02,
    GameEventType.VAR_REVIEW: 0.03,
    GameEventType.PENALTY: 0.02,
    GameEventType.OWN_GOAL: 0.01,
}


# =============================================================================
# Engagement Types (how viewers react)
# =============================================================================

class EngagementType(Enum):
    REACTION = "reaction"
    COMMENT = "comment"
    VIDEO_ACTION = "video_action"
    SHARE = "share"
    PREDICTION = "prediction"
    CLICK = "click"
    SESSION = "session"


# Subtypes for each engagement type
REACTION_SUBTYPES = ["cheer", "boo", "emoji_goal", "emoji_fire", "emoji_clap",
                     "emoji_cry", "emoji_angry", "emoji_heart", "emoji_laugh", "emoji_wow"]
COMMENT_SUBTYPES = ["match_commentary", "player_discussion", "team_support", "trash_talk", "question"]
VIDEO_SUBTYPES = ["pause", "play", "rewind", "replay", "camera_switch", "quality_change", "fullscreen"]
SHARE_SUBTYPES = ["twitter", "facebook", "whatsapp", "instagram", "in_app", "copy_link"]
PREDICTION_SUBTYPES = ["score_prediction", "next_goal", "player_rating", "poll_vote", "man_of_match"]
CLICK_SUBTYPES = ["stats_view", "player_profile", "team_info", "lineup", "ad_click", "merchandise"]


# Engagement multipliers based on game events
ENGAGEMENT_MULTIPLIERS = {
    GameEventType.GOAL: 15.0,
    GameEventType.OWN_GOAL: 12.0,
    GameEventType.RED_CARD: 10.0,
    GameEventType.PENALTY: 8.0,
    GameEventType.PENALTY_SAVED: 12.0,
    GameEventType.VAR_REVIEW: 6.0,
    GameEventType.SHOT_ON_TARGET: 4.0,
    GameEventType.SAVE: 3.0,
    GameEventType.YELLOW_CARD: 2.5,
    GameEventType.SHOT: 2.0,
    GameEventType.INJURY: 3.0,
    GameEventType.SUBSTITUTION: 1.8,
    GameEventType.FOUL: 1.3,
    GameEventType.CORNER: 1.2,
    GameEventType.FREE_KICK: 1.3,
    GameEventType.HALF_TIME: 3.0,
    GameEventType.FULL_TIME: 8.0,
    GameEventType.KICKOFF: 4.0,
    GameEventType.PASS: 1.0,
    GameEventType.INTERCEPTION: 1.1,
    GameEventType.TACKLE: 1.2,
    GameEventType.OFFSIDE: 1.1,
}


# =============================================================================
# Viewer Personas
# =============================================================================

@dataclass
class ViewerPersona:
    name: str
    base_engagement_rate: float  # engagements per minute
    reaction_weight: float
    comment_weight: float
    share_weight: float
    video_weight: float
    click_weight: float
    prediction_weight: float
    spike_sensitivity: float  # how much more engaged during exciting moments


PERSONAS = {
    "casual": ViewerPersona("casual", 0.3, 0.5, 0.2, 0.1, 0.3, 0.2, 0.1, 2.0),
    "active_fan": ViewerPersona("active_fan", 2.0, 2.0, 1.5, 0.8, 0.5, 1.0, 1.2, 5.0),
    "social_sharer": ViewerPersona("social_sharer", 1.5, 1.0, 0.5, 3.0, 1.5, 0.5, 0.5, 4.0),
    "stats_nerd": ViewerPersona("stats_nerd", 1.2, 0.5, 0.8, 0.3, 0.3, 4.0, 2.0, 2.0),
    "bettor": ViewerPersona("bettor", 1.8, 1.5, 0.5, 0.2, 0.8, 1.5, 5.0, 6.0),
}

PERSONA_DISTRIBUTION = {"casual": 0.55, "active_fan": 0.25, "social_sharer": 0.10,
                        "stats_nerd": 0.07, "bettor": 0.03}


# =============================================================================
# Data Classes
# =============================================================================

@dataclass
class Player:
    id: str
    name: str
    position: str
    number: int


@dataclass
class Team:
    id: int
    name: str
    players: List[Player]


@dataclass
class GameEvent:
    event_id: str
    match_id: str
    event_type: str
    team_id: int
    player_id: str
    game_minute: int
    metadata: Dict
    timestamp: str

    def to_api_format(self) -> Dict:
        """Format for the /api/events endpoint"""
        return {
            "eventId": self.event_id,
            "matchId": self.match_id,
            "eventType": self.event_type,
            "teamId": self.team_id,
            "playerId": self.player_id,
            "timestamp": self.timestamp,
            "metadata": self.metadata
        }


@dataclass
class EngagementEvent:
    event_id: str
    match_id: str
    user_id: str
    session_id: str
    engagement_type: str
    engagement_subtype: str
    related_game_event_id: Optional[str]
    game_minute: int
    device_type: str
    platform: str
    country_code: str
    content: str
    metadata: Dict
    timestamp: str

    def to_dict(self) -> Dict:
        return asdict(self)


@dataclass
class Viewer:
    user_id: str
    session_id: str
    persona: ViewerPersona
    device_type: str
    platform: str
    country_code: str
    team_affiliation: int  # 1 or 2, which team they support
    joined_minute: int
    is_active: bool = True
    engagement_count: int = 0


@dataclass
class SimulationStats:
    game_events_total: int = 0
    game_events_by_type: Dict[str, int] = field(default_factory=lambda: defaultdict(int))
    engagement_total: int = 0
    engagement_by_type: Dict[str, int] = field(default_factory=lambda: defaultdict(int))
    engagement_by_minute: Dict[int, int] = field(default_factory=lambda: defaultdict(int))
    viewers_current: int = 0
    viewers_peak: int = 0
    api_calls: int = 0
    api_errors: int = 0
    api_latency_sum: float = 0


# =============================================================================
# Team Data Loader
# =============================================================================

def load_team_from_csv(filepath: str, team_id: int, team_name: str) -> Team:
    """Load team data from CSV file"""
    players = []
    try:
        with open(filepath, 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                players.append(Player(
                    id=f"player_{team_id}_{row.get('Number', len(players)+1)}",
                    name=row.get('Name', f'Player {len(players)+1}'),
                    position=row.get('Position', 'MF'),
                    number=int(row.get('Number', len(players)+1))
                ))
    except Exception as e:
        logger.warning(f"Could not load team CSV {filepath}: {e}, using defaults")
        for i in range(1, 12):
            players.append(Player(
                id=f"player_{team_id}_{i}",
                name=f"Player {i}",
                position="MF",
                number=i
            ))

    return Team(id=team_id, name=team_name, players=players)


# =============================================================================
# Match Simulator
# =============================================================================

class MatchSimulator:
    """
    Simulates a full football match with:
    1. Realistic game events on the field
    2. 100K+ viewers generating engagement correlated to game events
    """

    def __init__(
        self,
        match_id: str,
        api_url: str,
        api_key: str,
        team1_csv: str,
        team2_csv: str,
        team1_name: str = "AlHilal",
        team2_name: str = "AlNassr",
        target_viewers: int = 100000,
        match_duration: int = 90,
        events_per_minute: Tuple[int, int] = (20, 50),
        engagement_batch_size: int = 500,
        concurrent_requests: int = 50
    ):
        self.match_id = match_id
        self.api_url = api_url.rstrip('/')
        self.api_key = api_key
        self.target_viewers = target_viewers
        self.match_duration = match_duration
        self.events_per_minute = events_per_minute
        self.engagement_batch_size = engagement_batch_size
        self.concurrent_requests = concurrent_requests

        # Load teams
        self.team1 = load_team_from_csv(team1_csv, 1, team1_name)
        self.team2 = load_team_from_csv(team2_csv, 2, team2_name)

        # State
        self.viewers: Dict[str, Viewer] = {}
        self.current_minute = 0
        self.score = {1: 0, 2: 0}
        self.stats = SimulationStats()

        # Device/platform distributions
        self.devices = ["mobile", "desktop", "tablet", "tv"]
        self.device_weights = [0.55, 0.25, 0.10, 0.10]
        self.platforms = {"mobile": ["ios", "android"], "desktop": ["web"],
                         "tablet": ["ios", "android"], "tv": ["smart_tv"]}
        self.countries = ["SA", "AE", "EG", "KW", "QA", "BH", "OM", "JO", "GB", "US"]
        self.country_weights = [0.35, 0.15, 0.10, 0.08, 0.07, 0.05, 0.05, 0.05, 0.05, 0.05]

        # Engagement content templates
        self.reaction_emojis = {
            "cheer": ["👏", "🎉", "🔥", "💪", "⚽"],
            "boo": ["👎", "😤", "🤬", "💢"],
            "emoji_goal": ["⚽⚽⚽", "GOOOAL!", "⚽🔥🎉"],
            "emoji_fire": ["🔥🔥🔥", "ON FIRE!", "🔥💯"],
            "emoji_wow": ["😮", "WOW!", "😱", "UNBELIEVABLE!"],
            "emoji_cry": ["😢", "💔", "😭"],
            "emoji_angry": ["😡", "🤬", "💢"],
        }

        self.comment_templates = [
            "What a play!", "Come on {team}!", "Should have scored!",
            "{player} is on fire!", "Terrible decision!", "VAR please!",
            "We need a goal!", "Great save!", "What was that?!",
            "Let's go!", "Defense!", "Pass it!", "Shoot!",
        ]

    def _select_persona(self) -> ViewerPersona:
        r = random.random()
        cumulative = 0
        for name, weight in PERSONA_DISTRIBUTION.items():
            cumulative += weight
            if r <= cumulative:
                return PERSONAS[name]
        return PERSONAS["casual"]

    def _create_viewer(self) -> Viewer:
        device = random.choices(self.devices, weights=self.device_weights)[0]
        platform = random.choice(self.platforms[device])
        country = random.choices(self.countries, weights=self.country_weights)[0]

        return Viewer(
            user_id=f"user_{uuid.uuid4().hex[:12]}",
            session_id=f"sess_{uuid.uuid4().hex[:8]}",
            persona=self._select_persona(),
            device_type=device,
            platform=platform,
            country_code=country,
            team_affiliation=random.choice([1, 2]),
            joined_minute=self.current_minute
        )

    def _select_game_event_type(self) -> GameEventType:
        """Select a game event type based on weights"""
        types = list(GAME_EVENT_WEIGHTS.keys())
        weights = list(GAME_EVENT_WEIGHTS.values())
        return random.choices(types, weights=weights)[0]

    def _generate_game_event(self, event_type: GameEventType) -> GameEvent:
        """Generate a single game event"""
        team = random.choice([self.team1, self.team2])
        player = random.choice(team.players)

        metadata = {"minute": self.current_minute}

        # Add event-specific metadata
        if event_type == GameEventType.GOAL:
            self.score[team.id] += 1
            metadata["score_home"] = self.score[1]
            metadata["score_away"] = self.score[2]
            # Sometimes add assist
            if random.random() < 0.7:
                assist_player = random.choice([p for p in team.players if p.id != player.id])
                metadata["assist"] = assist_player.name

        elif event_type == GameEventType.SHOT:
            metadata["on_target"] = random.random() < 0.4
            metadata["distance_meters"] = random.randint(10, 35)

        elif event_type == GameEventType.FOUL:
            metadata["severity"] = random.choice(["minor", "moderate", "severe"])

        elif event_type == GameEventType.YELLOW_CARD or event_type == GameEventType.RED_CARD:
            metadata["reason"] = random.choice(["foul", "unsporting", "dissent", "time_wasting"])

        elif event_type == GameEventType.SUBSTITUTION:
            sub_out = player
            sub_in = random.choice([p for p in team.players if p.id != sub_out.id])
            metadata["player_out"] = sub_out.name
            metadata["player_in"] = sub_in.name

        return GameEvent(
            event_id=str(uuid.uuid4()),
            match_id=self.match_id,
            event_type=event_type.value,
            team_id=team.id,
            player_id=player.id,
            game_minute=self.current_minute,
            metadata=metadata,
            timestamp=datetime.now(timezone.utc).isoformat()
        )

    def _generate_engagement(
        self,
        viewer: Viewer,
        game_event: Optional[GameEvent] = None
    ) -> EngagementEvent:
        """Generate an engagement event from a viewer"""
        # Select engagement type based on persona weights
        weights = {
            EngagementType.REACTION: viewer.persona.reaction_weight,
            EngagementType.COMMENT: viewer.persona.comment_weight,
            EngagementType.SHARE: viewer.persona.share_weight,
            EngagementType.VIDEO_ACTION: viewer.persona.video_weight,
            EngagementType.CLICK: viewer.persona.click_weight,
            EngagementType.PREDICTION: viewer.persona.prediction_weight,
        }

        total = sum(weights.values())
        r = random.random() * total
        cumulative = 0
        eng_type = EngagementType.REACTION

        for etype, weight in weights.items():
            cumulative += weight
            if r <= cumulative:
                eng_type = etype
                break

        # Generate subtype and content
        if eng_type == EngagementType.REACTION:
            # Different reactions based on game event
            if game_event and game_event.event_type == "goal":
                if game_event.team_id == viewer.team_affiliation:
                    subtype = random.choice(["cheer", "emoji_goal", "emoji_fire"])
                else:
                    subtype = random.choice(["boo", "emoji_cry", "emoji_angry"])
            else:
                subtype = random.choice(REACTION_SUBTYPES)
            content = random.choice(self.reaction_emojis.get(subtype, ["👍"]))

        elif eng_type == EngagementType.COMMENT:
            subtype = random.choice(COMMENT_SUBTYPES)
            template = random.choice(self.comment_templates)
            team = self.team1 if viewer.team_affiliation == 1 else self.team2
            player = random.choice(team.players)
            content = template.replace("{team}", team.name).replace("{player}", player.name)

        elif eng_type == EngagementType.VIDEO_ACTION:
            subtype = random.choice(VIDEO_SUBTYPES)
            content = ""

        elif eng_type == EngagementType.SHARE:
            subtype = random.choice(SHARE_SUBTYPES)
            content = ""

        elif eng_type == EngagementType.PREDICTION:
            subtype = random.choice(PREDICTION_SUBTYPES)
            if subtype == "score_prediction":
                content = f"{random.randint(0,4)}-{random.randint(0,4)}"
            else:
                content = random.choice(["yes", "no", "home", "away"])

        elif eng_type == EngagementType.CLICK:
            subtype = random.choice(CLICK_SUBTYPES)
            content = ""

        else:
            subtype = "unknown"
            content = ""

        return EngagementEvent(
            event_id=str(uuid.uuid4()),
            match_id=self.match_id,
            user_id=viewer.user_id,
            session_id=viewer.session_id,
            engagement_type=eng_type.value,
            engagement_subtype=subtype,
            related_game_event_id=game_event.event_id if game_event else None,
            game_minute=self.current_minute,
            device_type=viewer.device_type,
            platform=viewer.platform,
            country_code=viewer.country_code,
            content=content,
            metadata={"persona": viewer.persona.name},
            timestamp=datetime.now(timezone.utc).isoformat()
        )

    def _calculate_engagement_count(
        self,
        viewer: Viewer,
        game_events: List[GameEvent]
    ) -> int:
        """Calculate how many engagements a viewer generates this minute"""
        base_rate = viewer.persona.base_engagement_rate

        # Apply multipliers for exciting game events
        max_multiplier = 1.0
        for event in game_events:
            event_type = GameEventType(event.event_type)
            multiplier = ENGAGEMENT_MULTIPLIERS.get(event_type, 1.0)
            multiplier *= viewer.persona.spike_sensitivity
            max_multiplier = max(max_multiplier, multiplier)

        effective_rate = base_rate * max_multiplier

        # Poisson-like distribution
        return int(random.expovariate(1.0 / max(effective_rate, 0.1)))

    async def _send_game_events(
        self,
        session: aiohttp.ClientSession,
        events: List[GameEvent]
    ) -> bool:
        """Send game events to /api/events endpoint"""
        headers = {"Content-Type": "application/json", "X-API-Key": self.api_key}

        for event in events:
            try:
                start = time.time()
                async with session.post(
                    f"{self.api_url}/api/events",
                    json=event.to_api_format(),
                    headers=headers,
                    timeout=aiohttp.ClientTimeout(total=10)
                ) as response:
                    self.stats.api_calls += 1
                    self.stats.api_latency_sum += time.time() - start
                    if response.status not in [200, 201, 202]:
                        self.stats.api_errors += 1
            except Exception as e:
                self.stats.api_errors += 1
                logger.debug(f"Game event API error: {e}")

        return True

    async def _send_engagements(
        self,
        session: aiohttp.ClientSession,
        engagements: List[EngagementEvent]
    ) -> bool:
        """Send engagement events to /api/engagements endpoint"""
        if not engagements:
            return True

        headers = {"Content-Type": "application/json", "X-API-Key": self.api_key}
        payload = {"events": [e.to_dict() for e in engagements]}

        try:
            start = time.time()
            async with session.post(
                f"{self.api_url}/api/engagements",
                json=payload,
                headers=headers,
                timeout=aiohttp.ClientTimeout(total=30)
            ) as response:
                self.stats.api_calls += 1
                self.stats.api_latency_sum += time.time() - start
                if response.status not in [200, 201, 202]:
                    self.stats.api_errors += 1
                    return False
        except Exception as e:
            self.stats.api_errors += 1
            logger.debug(f"Engagement API error: {e}")
            return False

        return True

    async def _simulate_minute(
        self,
        session: aiohttp.ClientSession
    ) -> Tuple[List[GameEvent], List[EngagementEvent]]:
        """Simulate one minute of match and engagement"""
        game_events = []
        engagements = []

        # Generate game events for this minute
        num_events = random.randint(*self.events_per_minute)

        # Add mandatory events at specific minutes
        if self.current_minute == 0:
            game_events.append(self._generate_game_event(GameEventType.KICKOFF))
        elif self.current_minute == 45:
            game_events.append(self._generate_game_event(GameEventType.HALF_TIME))
        elif self.current_minute == 46:
            game_events.append(self._generate_game_event(GameEventType.SECOND_HALF))
        elif self.current_minute == 90:
            game_events.append(self._generate_game_event(GameEventType.FULL_TIME))

        # Generate random game events
        for _ in range(num_events):
            event_type = self._select_game_event_type()
            event = self._generate_game_event(event_type)
            game_events.append(event)

            self.stats.game_events_total += 1
            self.stats.game_events_by_type[event_type.value] += 1

        # Log significant events
        for event in game_events:
            if event.event_type in ["goal", "red_card", "penalty", "var_review", "kickoff", "half_time", "full_time"]:
                logger.info(f"Minute {self.current_minute}: {event.event_type.upper()} - Score: {self.score[1]}-{self.score[2]}")

        # Generate viewer engagements correlated to game events
        for user_id, viewer in list(self.viewers.items()):
            if not viewer.is_active:
                continue

            # Check for viewer drop-off
            watch_time = self.current_minute - viewer.joined_minute
            if watch_time > viewer.persona.base_engagement_rate * 30:  # attention decay
                if random.random() < 0.02:
                    viewer.is_active = False
                    del self.viewers[user_id]
                    continue

            # Generate engagements
            num_engagements = self._calculate_engagement_count(viewer, game_events)
            for _ in range(num_engagements):
                # Pick a game event to react to (if any)
                game_event = random.choice(game_events) if game_events and random.random() < 0.7 else None
                engagement = self._generate_engagement(viewer, game_event)
                engagements.append(engagement)
                viewer.engagement_count += 1

                self.stats.engagement_total += 1
                self.stats.engagement_by_type[engagement.engagement_type] += 1
                self.stats.engagement_by_minute[self.current_minute] += 1

        self.stats.viewers_current = len(self.viewers)
        self.stats.viewers_peak = max(self.stats.viewers_peak, len(self.viewers))

        return game_events, engagements

    async def _ramp_up_viewers(self, target: int, duration_seconds: int = 120):
        """Gradually add viewers"""
        logger.info(f"Ramping up to {target:,} viewers over {duration_seconds}s...")

        viewers_per_second = target / duration_seconds
        added = 0

        while added < target:
            batch = int(min(viewers_per_second, target - added))
            for _ in range(batch):
                viewer = self._create_viewer()
                self.viewers[viewer.user_id] = viewer
                added += 1

            if added % 10000 == 0:
                logger.info(f"  Added {added:,}/{target:,} viewers")

            await asyncio.sleep(1)

        logger.info(f"Ramp-up complete: {len(self.viewers):,} viewers active")

    async def run(self):
        """Run the full match simulation"""
        logger.info("=" * 60)
        logger.info(f"MATCH SIMULATION: {self.team1.name} vs {self.team2.name}")
        logger.info(f"Match ID: {self.match_id}")
        logger.info(f"Target Viewers: {self.target_viewers:,}")
        logger.info(f"Duration: {self.match_duration} minutes")
        logger.info("=" * 60)

        connector = aiohttp.TCPConnector(limit=self.concurrent_requests)
        async with aiohttp.ClientSession(connector=connector) as session:
            # Ramp up viewers before kickoff
            await self._ramp_up_viewers(self.target_viewers)

            # Simulate each minute
            for minute in range(self.match_duration + 1):
                self.current_minute = minute

                game_events, engagements = await self._simulate_minute(session)

                # Send game events (individually as they happen)
                await self._send_game_events(session, game_events)

                # Send engagements in batches
                for i in range(0, len(engagements), self.engagement_batch_size):
                    batch = engagements[i:i + self.engagement_batch_size]
                    await self._send_engagements(session, batch)

                # Progress log
                if minute % 5 == 0 or minute in [45, 90]:
                    logger.info(
                        f"Min {minute:2d} | Score: {self.score[1]}-{self.score[2]} | "
                        f"Viewers: {len(self.viewers):,} | "
                        f"Engagements: {self.stats.engagement_by_minute[minute]:,}"
                    )

                # Small delay to simulate real-time (remove for max throughput)
                # await asyncio.sleep(0.05)

        self._print_final_stats()

    def _print_final_stats(self):
        """Print comprehensive final statistics"""
        print("\n" + "=" * 70)
        print("MATCH SIMULATION COMPLETE")
        print("=" * 70)

        print(f"\n{'FINAL SCORE:':<20} {self.team1.name} {self.score[1]} - {self.score[2]} {self.team2.name}")
        print(f"{'Match ID:':<20} {self.match_id}")

        print("\n" + "-" * 70)
        print("GAME EVENTS ON FIELD")
        print("-" * 70)
        print(f"{'Total Events:':<20} {self.stats.game_events_total:,}")
        print("\nBy Type:")
        for etype, count in sorted(self.stats.game_events_by_type.items(), key=lambda x: -x[1])[:10]:
            print(f"  {etype:<20} {count:,}")

        print("\n" + "-" * 70)
        print("VIEWER ENGAGEMENT")
        print("-" * 70)
        print(f"{'Peak Viewers:':<20} {self.stats.viewers_peak:,}")
        print(f"{'Total Engagements:':<20} {self.stats.engagement_total:,}")
        print(f"{'Eng/Viewer:':<20} {self.stats.engagement_total / max(self.stats.viewers_peak, 1):.1f}")

        print("\nEngagement by Type:")
        for etype, count in sorted(self.stats.engagement_by_type.items(), key=lambda x: -x[1]):
            pct = count / max(self.stats.engagement_total, 1) * 100
            print(f"  {etype:<20} {count:>10,} ({pct:5.1f}%)")

        # Find peak engagement minute
        if self.stats.engagement_by_minute:
            peak_minute = max(self.stats.engagement_by_minute.items(), key=lambda x: x[1])
            print(f"\nPeak Engagement: Minute {peak_minute[0]} ({peak_minute[1]:,} engagements)")

        print("\n" + "-" * 70)
        print("API PERFORMANCE")
        print("-" * 70)
        print(f"{'Total Calls:':<20} {self.stats.api_calls:,}")
        print(f"{'Errors:':<20} {self.stats.api_errors:,}")
        if self.stats.api_calls > 0:
            print(f"{'Error Rate:':<20} {self.stats.api_errors/self.stats.api_calls*100:.2f}%")
            print(f"{'Avg Latency:':<20} {self.stats.api_latency_sum/self.stats.api_calls*1000:.1f}ms")

        print("=" * 70)


# =============================================================================
# Main
# =============================================================================

def main():
    parser = argparse.ArgumentParser(description="Football Match & Engagement Simulator")
    parser.add_argument("--match-id", default=f"match_{uuid.uuid4().hex[:8]}")
    parser.add_argument("--api-url", default="http://localhost:8080")
    parser.add_argument("--api-key", default="love-football-here")
    parser.add_argument("--team1-csv", default="alhilal.csv")
    parser.add_argument("--team2-csv", default="alnassr.csv")
    parser.add_argument("--team1-name", default="AlHilal")
    parser.add_argument("--team2-name", default="AlNassr")
    parser.add_argument("--viewers", type=int, default=100000)
    parser.add_argument("--duration", type=int, default=90)
    parser.add_argument("--batch-size", type=int, default=500)
    parser.add_argument("--concurrency", type=int, default=50)

    args = parser.parse_args()

    # Resolve CSV paths
    script_dir = Path(__file__).parent
    team1_csv = script_dir / args.team1_csv
    team2_csv = script_dir / args.team2_csv

    simulator = MatchSimulator(
        match_id=args.match_id,
        api_url=args.api_url,
        api_key=args.api_key,
        team1_csv=str(team1_csv),
        team2_csv=str(team2_csv),
        team1_name=args.team1_name,
        team2_name=args.team2_name,
        target_viewers=args.viewers,
        match_duration=args.duration,
        engagement_batch_size=args.batch_size,
        concurrent_requests=args.concurrency
    )

    asyncio.run(simulator.run())


if __name__ == "__main__":
    main()
