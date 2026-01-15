#!/usr/bin/env python3
"""
Viewer Engagement Simulator for Football Matches

Simulates 100K+ concurrent viewers watching a match and generating
engagement events (reactions, comments, shares, etc.) correlated
with game events.

This focuses on HOW people engage with the game, not just the game events.
"""

import asyncio
import aiohttp
import json
import random
import uuid
import time
import argparse
import logging
from dataclasses import dataclass, field, asdict
from typing import List, Dict, Optional, Tuple
from datetime import datetime, timezone
from enum import Enum
from collections import defaultdict
import sys

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# =============================================================================
# Engagement Types and Configuration
# =============================================================================

class EngagementType(Enum):
    REACTION = "reaction"
    COMMENT = "comment"
    VIDEO_ACTION = "video_action"
    SHARE = "share"
    PREDICTION = "prediction"
    CLICK = "click"
    SESSION = "session"


class ReactionSubtype(Enum):
    CHEER = "cheer"
    BOO = "boo"
    EMOJI_GOAL = "emoji_goal"       # ⚽
    EMOJI_FIRE = "emoji_fire"       # 🔥
    EMOJI_CLAP = "emoji_clap"       # 👏
    EMOJI_CRY = "emoji_cry"         # 😢
    EMOJI_ANGRY = "emoji_angry"     # 😡
    EMOJI_HEART = "emoji_heart"     # ❤️
    EMOJI_LAUGH = "emoji_laugh"     # 😂
    EMOJI_WOW = "emoji_wow"         # 😮


class CommentSubtype(Enum):
    MATCH_COMMENTARY = "match_commentary"
    PLAYER_DISCUSSION = "player_discussion"
    TEAM_SUPPORT = "team_support"
    TRASH_TALK = "trash_talk"
    QUESTION = "question"


class VideoActionSubtype(Enum):
    PAUSE = "pause"
    PLAY = "play"
    REWIND = "rewind"
    REPLAY = "replay"
    CAMERA_SWITCH = "camera_switch"
    QUALITY_CHANGE = "quality_change"
    FULLSCREEN = "fullscreen"


class ShareSubtype(Enum):
    TWITTER = "twitter"
    FACEBOOK = "facebook"
    WHATSAPP = "whatsapp"
    INSTAGRAM = "instagram"
    IN_APP = "in_app"
    COPY_LINK = "copy_link"


class PredictionSubtype(Enum):
    SCORE_PREDICTION = "score_prediction"
    NEXT_GOAL = "next_goal"
    PLAYER_RATING = "player_rating"
    POLL_VOTE = "poll_vote"
    MAN_OF_MATCH = "man_of_match"


class ClickSubtype(Enum):
    STATS_VIEW = "stats_view"
    PLAYER_PROFILE = "player_profile"
    TEAM_INFO = "team_info"
    LINEUP = "lineup"
    AD_CLICK = "ad_click"
    MERCHANDISE = "merchandise"
    TICKET = "ticket"


class DeviceType(Enum):
    MOBILE = "mobile"
    DESKTOP = "desktop"
    TABLET = "tablet"
    TV = "tv"


class Platform(Enum):
    IOS = "ios"
    ANDROID = "android"
    WEB = "web"
    SMART_TV = "smart_tv"


# =============================================================================
# User Personas - Different viewer behavior patterns
# =============================================================================

@dataclass
class UserPersona:
    """Defines engagement behavior patterns for different viewer types"""
    name: str
    base_engagement_rate: float  # engagements per minute baseline
    reaction_multiplier: float
    comment_multiplier: float
    share_multiplier: float
    video_action_multiplier: float
    click_multiplier: float
    prediction_multiplier: float
    attention_span_minutes: int  # how long before potential drop-off
    spike_sensitivity: float     # how much more engaged during exciting moments


# Persona definitions
PERSONAS = {
    "casual_viewer": UserPersona(
        name="casual_viewer",
        base_engagement_rate=0.3,
        reaction_multiplier=0.5,
        comment_multiplier=0.2,
        share_multiplier=0.1,
        video_action_multiplier=0.3,
        click_multiplier=0.2,
        prediction_multiplier=0.1,
        attention_span_minutes=60,
        spike_sensitivity=2.0
    ),
    "active_fan": UserPersona(
        name="active_fan",
        base_engagement_rate=2.0,
        reaction_multiplier=2.0,
        comment_multiplier=1.5,
        share_multiplier=0.8,
        video_action_multiplier=0.5,
        click_multiplier=1.0,
        prediction_multiplier=1.2,
        attention_span_minutes=95,
        spike_sensitivity=5.0
    ),
    "social_sharer": UserPersona(
        name="social_sharer",
        base_engagement_rate=1.5,
        reaction_multiplier=1.0,
        comment_multiplier=0.5,
        share_multiplier=3.0,
        video_action_multiplier=1.5,  # replays to capture moments
        click_multiplier=0.5,
        prediction_multiplier=0.5,
        attention_span_minutes=75,
        spike_sensitivity=4.0
    ),
    "stats_enthusiast": UserPersona(
        name="stats_enthusiast",
        base_engagement_rate=1.2,
        reaction_multiplier=0.5,
        comment_multiplier=0.8,
        share_multiplier=0.3,
        video_action_multiplier=0.3,
        click_multiplier=4.0,  # loves checking stats
        prediction_multiplier=2.0,
        attention_span_minutes=90,
        spike_sensitivity=2.0
    ),
    "bettor": UserPersona(
        name="bettor",
        base_engagement_rate=1.8,
        reaction_multiplier=1.5,
        comment_multiplier=0.5,
        share_multiplier=0.2,
        video_action_multiplier=0.8,
        click_multiplier=1.5,
        prediction_multiplier=5.0,
        attention_span_minutes=95,
        spike_sensitivity=6.0  # very reactive to game-changing moments
    )
}

# Persona distribution (what percentage of viewers are each type)
PERSONA_DISTRIBUTION = {
    "casual_viewer": 0.55,
    "active_fan": 0.25,
    "social_sharer": 0.10,
    "stats_enthusiast": 0.07,
    "bettor": 0.03
}

# Device distribution
DEVICE_DISTRIBUTION = {
    DeviceType.MOBILE: 0.55,
    DeviceType.DESKTOP: 0.25,
    DeviceType.TABLET: 0.10,
    DeviceType.TV: 0.10
}

# Platform by device
PLATFORM_BY_DEVICE = {
    DeviceType.MOBILE: {Platform.IOS: 0.45, Platform.ANDROID: 0.55},
    DeviceType.DESKTOP: {Platform.WEB: 1.0},
    DeviceType.TABLET: {Platform.IOS: 0.55, Platform.ANDROID: 0.45},
    DeviceType.TV: {Platform.SMART_TV: 1.0}
}


# =============================================================================
# Game Event Impact on Engagement
# =============================================================================

# How much each game event type multiplies engagement
GAME_EVENT_IMPACT = {
    "goal": 15.0,
    "red_card": 10.0,
    "penalty": 8.0,
    "own_goal": 12.0,
    "saved_penalty": 10.0,
    "yellow_card": 3.0,
    "shot": 2.0,
    "shot_on_target": 4.0,
    "near_miss": 5.0,
    "foul": 1.5,
    "corner": 1.3,
    "free_kick": 1.5,
    "offside": 1.2,
    "substitution": 2.0,
    "var_review": 8.0,
    "injury": 3.0,
    "fight": 7.0,
    "celebration": 6.0,
    "half_time": 4.0,
    "full_time": 10.0,
    "kickoff": 5.0,
    "pass": 1.0,
    "interception": 1.2
}

# Engagement type likelihood during different events
# (which types of engagement spike for which events)
EVENT_ENGAGEMENT_PROFILE = {
    "goal": {
        EngagementType.REACTION: 0.70,
        EngagementType.COMMENT: 0.15,
        EngagementType.SHARE: 0.08,
        EngagementType.VIDEO_ACTION: 0.05,
        EngagementType.PREDICTION: 0.02
    },
    "red_card": {
        EngagementType.REACTION: 0.60,
        EngagementType.COMMENT: 0.25,
        EngagementType.SHARE: 0.05,
        EngagementType.VIDEO_ACTION: 0.08,
        EngagementType.PREDICTION: 0.02
    },
    "shot": {
        EngagementType.REACTION: 0.80,
        EngagementType.VIDEO_ACTION: 0.15,
        EngagementType.COMMENT: 0.05
    },
    "default": {
        EngagementType.REACTION: 0.60,
        EngagementType.COMMENT: 0.15,
        EngagementType.VIDEO_ACTION: 0.10,
        EngagementType.CLICK: 0.08,
        EngagementType.SHARE: 0.04,
        EngagementType.PREDICTION: 0.03
    }
}


# =============================================================================
# Data Classes
# =============================================================================

@dataclass
class Viewer:
    """Represents a single viewer watching the match"""
    user_id: str
    session_id: str
    persona: UserPersona
    device_type: DeviceType
    platform: Platform
    country_code: str
    joined_minute: int
    is_active: bool = True
    engagement_count: int = 0
    last_engagement_time: float = 0


@dataclass
class EngagementEvent:
    """A single engagement event from a viewer"""
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
class GameEvent:
    """A game event that can trigger engagement"""
    event_id: str
    match_id: str
    event_type: str
    team_id: int
    player_id: str
    game_minute: int
    metadata: Dict
    timestamp: str


@dataclass
class SimulationStats:
    """Tracks simulation statistics"""
    total_engagements: int = 0
    engagements_by_type: Dict[str, int] = field(default_factory=lambda: defaultdict(int))
    engagements_by_minute: Dict[int, int] = field(default_factory=lambda: defaultdict(int))
    active_viewers: int = 0
    peak_viewers: int = 0
    peak_engagement_minute: int = 0
    peak_engagement_count: int = 0
    api_calls: int = 0
    api_errors: int = 0
    api_latency_sum: float = 0


# =============================================================================
# Engagement Generator
# =============================================================================

class EngagementGenerator:
    """Generates realistic engagement events based on viewer behavior"""

    def __init__(self):
        self.reaction_templates = {
            ReactionSubtype.CHEER: ["👏", "🎉", "⚽", "🔥", "💪"],
            ReactionSubtype.BOO: ["👎", "😤", "🤬"],
            ReactionSubtype.EMOJI_GOAL: ["⚽⚽⚽", "GOOOAL!", "⚽🔥"],
            ReactionSubtype.EMOJI_FIRE: ["🔥🔥🔥", "ON FIRE!"],
            ReactionSubtype.EMOJI_WOW: ["😮", "WOW!", "UNBELIEVABLE!"],
        }

        self.comment_templates = {
            CommentSubtype.MATCH_COMMENTARY: [
                "Great play!",
                "Should have been a goal",
                "What a save!",
                "Come on!",
                "That was close!",
                "Terrible decision",
                "VAR?!",
                "We need more of that",
            ],
            CommentSubtype.TEAM_SUPPORT: [
                "Let's go {team}!",
                "{team} forever!",
                "Come on {team}!",
                "We got this!",
            ],
            CommentSubtype.PLAYER_DISCUSSION: [
                "{player} is on fire today",
                "Sub out {player}",
                "{player} needs to step up",
                "Give it to {player}!",
            ]
        }

    def generate_reaction(self, viewer: Viewer, game_event: Optional[GameEvent], game_minute: int) -> Tuple[str, str]:
        """Generate a reaction subtype and content"""
        if game_event and game_event.event_type == "goal":
            if game_event.team_id == 1:  # viewer's team
                subtype = random.choice([ReactionSubtype.CHEER, ReactionSubtype.EMOJI_GOAL, ReactionSubtype.EMOJI_FIRE])
            else:
                subtype = random.choice([ReactionSubtype.BOO, ReactionSubtype.EMOJI_CRY, ReactionSubtype.EMOJI_ANGRY])
        elif game_event and game_event.event_type in ["red_card", "foul"]:
            subtype = random.choice([ReactionSubtype.EMOJI_ANGRY, ReactionSubtype.EMOJI_WOW, ReactionSubtype.BOO])
        else:
            subtype = random.choice(list(ReactionSubtype))

        content = random.choice(self.reaction_templates.get(subtype, ["👍"]))
        return subtype.value, content

    def generate_comment(self, viewer: Viewer, game_event: Optional[GameEvent]) -> Tuple[str, str]:
        """Generate a comment subtype and content"""
        subtype = random.choice(list(CommentSubtype))
        templates = self.comment_templates.get(subtype, ["Nice!"])
        content = random.choice(templates)

        # Replace placeholders
        content = content.replace("{team}", random.choice(["AlHilal", "AlNassr"]))
        content = content.replace("{player}", f"Player{random.randint(1, 22)}")

        return subtype.value, content

    def generate_video_action(self) -> Tuple[str, str]:
        """Generate a video action"""
        subtype = random.choice(list(VideoActionSubtype))
        content = ""
        return subtype.value, content

    def generate_share(self) -> Tuple[str, str]:
        """Generate a share action"""
        subtype = random.choice(list(ShareSubtype))
        content = ""
        return subtype.value, content

    def generate_prediction(self, game_minute: int) -> Tuple[str, str]:
        """Generate a prediction"""
        subtype = random.choice(list(PredictionSubtype))
        if subtype == PredictionSubtype.SCORE_PREDICTION:
            content = f"{random.randint(0,3)}-{random.randint(0,3)}"
        elif subtype == PredictionSubtype.NEXT_GOAL:
            content = random.choice(["home", "away", "no_goal"])
        elif subtype == PredictionSubtype.PLAYER_RATING:
            content = str(random.randint(1, 10))
        else:
            content = random.choice(["yes", "no"])
        return subtype.value, content

    def generate_click(self) -> Tuple[str, str]:
        """Generate a click action"""
        subtype = random.choice(list(ClickSubtype))
        content = ""
        return subtype.value, content


# =============================================================================
# Viewer Simulator
# =============================================================================

class ViewerSimulator:
    """Simulates concurrent viewers with realistic engagement patterns"""

    def __init__(
        self,
        match_id: str,
        api_url: str,
        api_key: str,
        target_viewers: int = 100000,
        match_duration_minutes: int = 90,
        batch_size: int = 500,
        concurrent_requests: int = 50
    ):
        self.match_id = match_id
        self.api_url = api_url.rstrip('/')
        self.api_key = api_key
        self.target_viewers = target_viewers
        self.match_duration_minutes = match_duration_minutes
        self.batch_size = batch_size
        self.concurrent_requests = concurrent_requests

        self.viewers: Dict[str, Viewer] = {}
        self.engagement_generator = EngagementGenerator()
        self.stats = SimulationStats()
        self.current_game_events: List[GameEvent] = []
        self.current_minute = 0

        # Country distribution (for realism)
        self.countries = ["SA", "AE", "EG", "KW", "QA", "BH", "OM", "JO", "GB", "US", "FR", "DE", "ES"]
        self.country_weights = [0.35, 0.15, 0.10, 0.08, 0.07, 0.05, 0.04, 0.04, 0.03, 0.03, 0.02, 0.02, 0.02]

    def _select_persona(self) -> UserPersona:
        """Select a persona based on distribution"""
        r = random.random()
        cumulative = 0
        for persona_name, weight in PERSONA_DISTRIBUTION.items():
            cumulative += weight
            if r <= cumulative:
                return PERSONAS[persona_name]
        return PERSONAS["casual_viewer"]

    def _select_device(self) -> Tuple[DeviceType, Platform]:
        """Select device and platform"""
        r = random.random()
        cumulative = 0
        device = DeviceType.MOBILE
        for dev, weight in DEVICE_DISTRIBUTION.items():
            cumulative += weight
            if r <= cumulative:
                device = dev
                break

        platforms = PLATFORM_BY_DEVICE[device]
        r = random.random()
        cumulative = 0
        for plat, weight in platforms.items():
            cumulative += weight
            if r <= cumulative:
                return device, plat

        return device, Platform.WEB

    def create_viewer(self, join_minute: int) -> Viewer:
        """Create a new viewer with random characteristics"""
        user_id = f"user_{uuid.uuid4().hex[:12]}"
        session_id = f"sess_{uuid.uuid4().hex[:8]}"
        persona = self._select_persona()
        device, platform = self._select_device()
        country = random.choices(self.countries, weights=self.country_weights)[0]

        return Viewer(
            user_id=user_id,
            session_id=session_id,
            persona=persona,
            device_type=device,
            platform=platform,
            country_code=country,
            joined_minute=join_minute
        )

    def should_viewer_drop_off(self, viewer: Viewer) -> bool:
        """Determine if a viewer should leave based on attention span"""
        watch_time = self.current_minute - viewer.joined_minute
        attention = viewer.persona.attention_span_minutes

        # Probability of drop-off increases as watch time exceeds attention span
        if watch_time < attention * 0.5:
            return random.random() < 0.001
        elif watch_time < attention:
            return random.random() < 0.01
        else:
            # Past attention span, higher chance of leaving
            return random.random() < 0.05

    def calculate_engagement_probability(
        self,
        viewer: Viewer,
        game_event: Optional[GameEvent] = None
    ) -> float:
        """Calculate probability of viewer engaging based on context"""
        base_rate = viewer.persona.base_engagement_rate / 60  # per second

        # Apply game event multiplier if present
        if game_event:
            event_impact = GAME_EVENT_IMPACT.get(game_event.event_type, 1.0)
            base_rate *= event_impact * viewer.persona.spike_sensitivity

        # Half-time boost (people check stats, make predictions)
        if self.current_minute in [45, 46, 47]:
            base_rate *= 2.0

        # End of match boost
        if self.current_minute >= 85:
            base_rate *= 1.5

        return min(base_rate, 0.5)  # Cap at 50% per second

    def select_engagement_type(
        self,
        viewer: Viewer,
        game_event: Optional[GameEvent] = None
    ) -> EngagementType:
        """Select what type of engagement based on viewer persona and context"""
        # Get base profile for current context
        if game_event:
            profile = EVENT_ENGAGEMENT_PROFILE.get(
                game_event.event_type,
                EVENT_ENGAGEMENT_PROFILE["default"]
            )
        else:
            profile = EVENT_ENGAGEMENT_PROFILE["default"]

        # Adjust by persona multipliers
        weights = {}
        weights[EngagementType.REACTION] = profile.get(EngagementType.REACTION, 0) * viewer.persona.reaction_multiplier
        weights[EngagementType.COMMENT] = profile.get(EngagementType.COMMENT, 0) * viewer.persona.comment_multiplier
        weights[EngagementType.SHARE] = profile.get(EngagementType.SHARE, 0) * viewer.persona.share_multiplier
        weights[EngagementType.VIDEO_ACTION] = profile.get(EngagementType.VIDEO_ACTION, 0) * viewer.persona.video_action_multiplier
        weights[EngagementType.CLICK] = profile.get(EngagementType.CLICK, 0) * viewer.persona.click_multiplier
        weights[EngagementType.PREDICTION] = profile.get(EngagementType.PREDICTION, 0) * viewer.persona.prediction_multiplier

        # Normalize and select
        total = sum(weights.values())
        if total == 0:
            return EngagementType.REACTION

        r = random.random() * total
        cumulative = 0
        for eng_type, weight in weights.items():
            cumulative += weight
            if r <= cumulative:
                return eng_type

        return EngagementType.REACTION

    def generate_engagement(
        self,
        viewer: Viewer,
        game_event: Optional[GameEvent] = None
    ) -> EngagementEvent:
        """Generate a single engagement event"""
        eng_type = self.select_engagement_type(viewer, game_event)

        # Generate content based on type
        if eng_type == EngagementType.REACTION:
            subtype, content = self.engagement_generator.generate_reaction(
                viewer, game_event, self.current_minute
            )
        elif eng_type == EngagementType.COMMENT:
            subtype, content = self.engagement_generator.generate_comment(viewer, game_event)
        elif eng_type == EngagementType.VIDEO_ACTION:
            subtype, content = self.engagement_generator.generate_video_action()
        elif eng_type == EngagementType.SHARE:
            subtype, content = self.engagement_generator.generate_share()
        elif eng_type == EngagementType.PREDICTION:
            subtype, content = self.engagement_generator.generate_prediction(self.current_minute)
        elif eng_type == EngagementType.CLICK:
            subtype, content = self.engagement_generator.generate_click()
        else:
            subtype, content = "unknown", ""

        return EngagementEvent(
            event_id=str(uuid.uuid4()),
            match_id=self.match_id,
            user_id=viewer.user_id,
            session_id=viewer.session_id,
            engagement_type=eng_type.value,
            engagement_subtype=subtype,
            related_game_event_id=game_event.event_id if game_event else None,
            game_minute=self.current_minute,
            device_type=viewer.device_type.value,
            platform=viewer.platform.value,
            country_code=viewer.country_code,
            content=content,
            metadata={
                "persona": viewer.persona.name,
                "watch_time_minutes": self.current_minute - viewer.joined_minute
            },
            timestamp=datetime.now(timezone.utc).isoformat()
        )

    async def send_engagement_batch(
        self,
        session: aiohttp.ClientSession,
        engagements: List[EngagementEvent]
    ) -> bool:
        """Send a batch of engagements to the API"""
        if not engagements:
            return True

        headers = {
            "Content-Type": "application/json",
            "X-API-Key": self.api_key
        }

        payload = {
            "events": [e.to_dict() for e in engagements]
        }

        try:
            start_time = time.time()
            async with session.post(
                f"{self.api_url}/api/engagements",
                json=payload,
                headers=headers,
                timeout=aiohttp.ClientTimeout(total=30)
            ) as response:
                latency = time.time() - start_time
                self.stats.api_calls += 1
                self.stats.api_latency_sum += latency

                if response.status in [200, 201, 202]:
                    return True
                else:
                    self.stats.api_errors += 1
                    logger.warning(f"API returned {response.status}: {await response.text()}")
                    return False

        except Exception as e:
            self.stats.api_errors += 1
            logger.error(f"API error: {e}")
            return False

    async def send_session_event(
        self,
        session: aiohttp.ClientSession,
        viewer: Viewer,
        action: str  # "join", "leave", "heartbeat"
    ):
        """Send a session event for a viewer"""
        event = EngagementEvent(
            event_id=str(uuid.uuid4()),
            match_id=self.match_id,
            user_id=viewer.user_id,
            session_id=viewer.session_id,
            engagement_type="session",
            engagement_subtype=action,
            related_game_event_id=None,
            game_minute=self.current_minute,
            device_type=viewer.device_type.value,
            platform=viewer.platform.value,
            country_code=viewer.country_code,
            content="",
            metadata={},
            timestamp=datetime.now(timezone.utc).isoformat()
        )

        await self.send_engagement_batch(session, [event])

    def inject_game_event(
        self,
        event_type: str,
        team_id: int = 1,
        player_id: str = "player_1",
        metadata: Optional[Dict] = None
    ):
        """Inject a game event that will trigger engagement spikes"""
        event = GameEvent(
            event_id=str(uuid.uuid4()),
            match_id=self.match_id,
            event_type=event_type,
            team_id=team_id,
            player_id=player_id,
            game_minute=self.current_minute,
            metadata=metadata or {},
            timestamp=datetime.now(timezone.utc).isoformat()
        )
        self.current_game_events.append(event)
        logger.info(f"Minute {self.current_minute}: Game event - {event_type}")

    async def simulate_minute(
        self,
        session: aiohttp.ClientSession,
        game_events: List[GameEvent]
    ) -> List[EngagementEvent]:
        """Simulate one minute of viewer engagement"""
        engagements = []

        # Process each viewer
        viewers_to_remove = []
        for user_id, viewer in self.viewers.items():
            if not viewer.is_active:
                continue

            # Check for drop-off
            if self.should_viewer_drop_off(viewer):
                viewer.is_active = False
                viewers_to_remove.append(user_id)
                await self.send_session_event(session, viewer, "leave")
                continue

            # Generate engagements (simulate ~60 seconds with varying frequency)
            for _ in range(60):  # 60 seconds in a minute
                # Check if viewer engages
                for game_event in (game_events or [None]):
                    prob = self.calculate_engagement_probability(viewer, game_event)
                    if random.random() < prob:
                        engagement = self.generate_engagement(viewer, game_event)
                        engagements.append(engagement)
                        viewer.engagement_count += 1

                        # Update stats
                        self.stats.total_engagements += 1
                        self.stats.engagements_by_type[engagement.engagement_type] += 1
                        self.stats.engagements_by_minute[self.current_minute] += 1

        # Clean up inactive viewers
        for user_id in viewers_to_remove:
            del self.viewers[user_id]

        self.stats.active_viewers = len(self.viewers)
        if len(self.viewers) > self.stats.peak_viewers:
            self.stats.peak_viewers = len(self.viewers)

        minute_count = self.stats.engagements_by_minute[self.current_minute]
        if minute_count > self.stats.peak_engagement_count:
            self.stats.peak_engagement_count = minute_count
            self.stats.peak_engagement_minute = self.current_minute

        return engagements

    async def ramp_up_viewers(
        self,
        session: aiohttp.ClientSession,
        target: int,
        duration_seconds: int = 300
    ):
        """Gradually add viewers to reach target count"""
        current = len(self.viewers)
        to_add = target - current
        if to_add <= 0:
            return

        viewers_per_second = to_add / duration_seconds
        interval = 1.0 / max(viewers_per_second, 1)

        logger.info(f"Ramping up from {current} to {target} viewers over {duration_seconds}s")

        added = 0
        start_time = time.time()

        while added < to_add and (time.time() - start_time) < duration_seconds:
            viewer = self.create_viewer(self.current_minute)
            self.viewers[viewer.user_id] = viewer

            # Send join event (batch them for efficiency)
            if added % 100 == 0:
                await self.send_session_event(session, viewer, "join")

            added += 1

            if added % 10000 == 0:
                logger.info(f"Added {added}/{to_add} viewers")

            await asyncio.sleep(interval)

        logger.info(f"Ramp-up complete: {len(self.viewers)} viewers active")

    async def run_simulation(self, game_event_script: Optional[List[Dict]] = None):
        """Run the full match simulation"""
        logger.info(f"Starting simulation for match {self.match_id}")
        logger.info(f"Target viewers: {self.target_viewers}")
        logger.info(f"Match duration: {self.match_duration_minutes} minutes")

        # Default game event script if not provided
        if game_event_script is None:
            game_event_script = self._default_game_script()

        connector = aiohttp.TCPConnector(limit=self.concurrent_requests)
        async with aiohttp.ClientSession(connector=connector) as session:
            # Ramp up viewers before kickoff
            await self.ramp_up_viewers(session, self.target_viewers, duration_seconds=120)

            # Simulate match
            for minute in range(self.match_duration_minutes + 1):
                self.current_minute = minute
                self.current_game_events = []

                # Check for scripted game events
                for event in game_event_script:
                    if event.get("minute") == minute:
                        self.inject_game_event(
                            event["type"],
                            event.get("team_id", 1),
                            event.get("player_id", "unknown"),
                            event.get("metadata")
                        )

                # Simulate viewer engagement
                engagements = await self.simulate_minute(session, self.current_game_events)

                # Send engagements in batches
                for i in range(0, len(engagements), self.batch_size):
                    batch = engagements[i:i + self.batch_size]
                    await self.send_engagement_batch(session, batch)

                # Log progress
                if minute % 5 == 0:
                    logger.info(
                        f"Minute {minute}: {self.stats.active_viewers} viewers, "
                        f"{self.stats.engagements_by_minute[minute]} engagements, "
                        f"Total: {self.stats.total_engagements}"
                    )

                # Simulate real-time (optional, remove for max speed)
                # await asyncio.sleep(0.1)

            # Final stats
            self._print_final_stats()

    def _default_game_script(self) -> List[Dict]:
        """Default realistic game event script"""
        return [
            {"minute": 0, "type": "kickoff"},
            {"minute": 12, "type": "shot", "team_id": 1},
            {"minute": 23, "type": "goal", "team_id": 1, "player_id": "player_7"},
            {"minute": 28, "type": "foul", "team_id": 2},
            {"minute": 35, "type": "yellow_card", "team_id": 2},
            {"minute": 41, "type": "shot", "team_id": 2},
            {"minute": 45, "type": "half_time"},
            {"minute": 52, "type": "goal", "team_id": 2, "player_id": "player_9"},
            {"minute": 58, "type": "shot", "team_id": 1},
            {"minute": 63, "type": "substitution", "team_id": 1},
            {"minute": 71, "type": "goal", "team_id": 1, "player_id": "player_11"},
            {"minute": 76, "type": "red_card", "team_id": 2, "player_id": "player_3"},
            {"minute": 82, "type": "shot", "team_id": 1},
            {"minute": 88, "type": "near_miss", "team_id": 2},
            {"minute": 90, "type": "full_time"},
        ]

    def _print_final_stats(self):
        """Print final simulation statistics"""
        print("\n" + "=" * 60)
        print("SIMULATION COMPLETE - ENGAGEMENT STATISTICS")
        print("=" * 60)
        print(f"\nMatch ID: {self.match_id}")
        print(f"Total Viewers (peak): {self.stats.peak_viewers:,}")
        print(f"Total Engagements: {self.stats.total_engagements:,}")
        print(f"\nEngagements by Type:")
        for eng_type, count in sorted(self.stats.engagements_by_type.items(), key=lambda x: -x[1]):
            pct = count / max(self.stats.total_engagements, 1) * 100
            print(f"  {eng_type}: {count:,} ({pct:.1f}%)")
        print(f"\nPeak Engagement: Minute {self.stats.peak_engagement_minute} "
              f"({self.stats.peak_engagement_count:,} engagements)")
        print(f"\nAPI Stats:")
        print(f"  Total Calls: {self.stats.api_calls:,}")
        print(f"  Errors: {self.stats.api_errors:,}")
        if self.stats.api_calls > 0:
            avg_latency = self.stats.api_latency_sum / self.stats.api_calls * 1000
            print(f"  Avg Latency: {avg_latency:.1f}ms")
        print("=" * 60)


# =============================================================================
# Main Entry Point
# =============================================================================

def main():
    parser = argparse.ArgumentParser(
        description="Simulate viewer engagement for football matches"
    )
    parser.add_argument(
        "--match-id",
        default=f"match_{uuid.uuid4().hex[:8]}",
        help="Match ID for the simulation"
    )
    parser.add_argument(
        "--api-url",
        default="http://localhost:8080",
        help="Base URL of the API"
    )
    parser.add_argument(
        "--api-key",
        default="love-football-here",
        help="API key for authentication"
    )
    parser.add_argument(
        "--viewers",
        type=int,
        default=100000,
        help="Target number of concurrent viewers"
    )
    parser.add_argument(
        "--duration",
        type=int,
        default=90,
        help="Match duration in minutes"
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=500,
        help="Batch size for API calls"
    )
    parser.add_argument(
        "--concurrency",
        type=int,
        default=50,
        help="Number of concurrent API connections"
    )

    args = parser.parse_args()

    simulator = ViewerSimulator(
        match_id=args.match_id,
        api_url=args.api_url,
        api_key=args.api_key,
        target_viewers=args.viewers,
        match_duration_minutes=args.duration,
        batch_size=args.batch_size,
        concurrent_requests=args.concurrency
    )

    asyncio.run(simulator.run_simulation())


if __name__ == "__main__":
    main()
