package domain

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

// EngagementType represents the type of user engagement.
type EngagementType string

// Engagement type constants for all supported engagement types.
const (
	EngagementTypeReaction    EngagementType = "reaction"
	EngagementTypeComment     EngagementType = "comment"
	EngagementTypeVideoAction EngagementType = "video_action"
	EngagementTypeShare       EngagementType = "share"
	EngagementTypePrediction  EngagementType = "prediction"
	EngagementTypeClick       EngagementType = "click"
	EngagementTypeSession     EngagementType = "session"
)

// ValidEngagementTypes is a map of all valid engagement types for validation.
var ValidEngagementTypes = map[EngagementType]bool{
	EngagementTypeReaction:    true,
	EngagementTypeComment:     true,
	EngagementTypeVideoAction: true,
	EngagementTypeShare:       true,
	EngagementTypePrediction:  true,
	EngagementTypeClick:       true,
	EngagementTypeSession:     true,
}

// EngagementSubtypes defines valid subtypes for each engagement type.
var EngagementSubtypes = map[EngagementType]map[string]bool{
	EngagementTypeReaction: {
		"cheer": true, "boo": true, "emoji_goal": true, "emoji_fire": true,
		"emoji_clap": true, "emoji_cry": true, "emoji_angry": true,
		"emoji_heart": true, "emoji_laugh": true, "emoji_wow": true,
	},
	EngagementTypeComment: {
		"match_commentary": true, "player_discussion": true, "team_support": true,
		"trash_talk": true, "question": true,
	},
	EngagementTypeVideoAction: {
		"pause": true, "play": true, "rewind": true, "replay": true,
		"camera_switch": true, "quality_change": true, "fullscreen": true,
	},
	EngagementTypeShare: {
		"twitter": true, "facebook": true, "whatsapp": true, "instagram": true,
		"in_app": true, "copy_link": true,
	},
	EngagementTypePrediction: {
		"score_prediction": true, "next_goal": true, "player_rating": true,
		"poll_vote": true, "man_of_match": true,
	},
	EngagementTypeClick: {
		"stats_view": true, "player_profile": true, "team_info": true,
		"lineup": true, "ad_click": true, "merchandise": true, "ticket": true,
	},
	EngagementTypeSession: {
		"join": true, "leave": true, "heartbeat": true, "reconnect": true,
	},
}

// DeviceType represents the type of device.
type DeviceType string

const (
	DeviceMobile  DeviceType = "mobile"
	DeviceDesktop DeviceType = "desktop"
	DeviceTablet  DeviceType = "tablet"
	DeviceTV      DeviceType = "tv"
	DeviceUnknown DeviceType = "unknown"
)

// ValidDeviceTypes is a map of all valid device types.
var ValidDeviceTypes = map[DeviceType]bool{
	DeviceMobile:  true,
	DeviceDesktop: true,
	DeviceTablet:  true,
	DeviceTV:      true,
	DeviceUnknown: true,
}

// EngagementEvent represents a validated user engagement event.
type EngagementEvent struct {
	EventID            uuid.UUID
	MatchID            string
	UserID             string
	SessionID          string
	EngagementType     EngagementType
	EngagementSubtype  string
	RelatedGameEventID *uuid.UUID
	GameMinute         int
	DeviceType         DeviceType
	Platform           string
	CountryCode        string
	Content            string
	Metadata           map[string]interface{}
	Timestamp          time.Time
}

// EngagementEventRequest represents the incoming JSON request for an engagement event.
type EngagementEventRequest struct {
	EventID            string                 `json:"event_id"`
	MatchID            string                 `json:"match_id"`
	UserID             string                 `json:"user_id"`
	SessionID          string                 `json:"session_id"`
	EngagementType     string                 `json:"engagement_type"`
	EngagementSubtype  string                 `json:"engagement_subtype"`
	RelatedGameEventID *string                `json:"related_game_event_id,omitempty"`
	GameMinute         int                    `json:"game_minute"`
	DeviceType         string                 `json:"device_type"`
	Platform           string                 `json:"platform"`
	CountryCode        string                 `json:"country_code"`
	Content            string                 `json:"content"`
	Metadata           map[string]interface{} `json:"metadata,omitempty"`
	Timestamp          string                 `json:"timestamp"`
}

// EngagementBatchRequest represents a batch of engagement events.
type EngagementBatchRequest struct {
	Events []EngagementEventRequest `json:"events"`
}

// ToEngagementEvent validates and converts a request to a domain EngagementEvent.
func (r *EngagementEventRequest) ToEngagementEvent() (*EngagementEvent, error) {
	// Parse and validate UUID
	eventUUID, err := uuid.Parse(r.EventID)
	if err != nil {
		return nil, NewValidationError("event_id", "must be a valid UUID")
	}

	// Validate required fields
	if r.MatchID == "" {
		return nil, NewValidationError("match_id", "is required")
	}
	if r.UserID == "" {
		return nil, NewValidationError("user_id", "is required")
	}
	if r.SessionID == "" {
		return nil, NewValidationError("session_id", "is required")
	}

	// Validate engagement type
	engagementType := EngagementType(r.EngagementType)
	if !ValidEngagementTypes[engagementType] {
		return nil, NewValidationError("engagement_type", "must be a valid engagement type")
	}

	// Subtype validation: subtypes are optional and unknown subtypes are allowed

	// Parse related game event ID if provided
	var relatedGameEventID *uuid.UUID
	if r.RelatedGameEventID != nil && *r.RelatedGameEventID != "" {
		parsed, err := uuid.Parse(*r.RelatedGameEventID)
		if err != nil {
			return nil, NewValidationError("related_game_event_id", "must be a valid UUID")
		}
		relatedGameEventID = &parsed
	}

	// Validate device type
	deviceType := DeviceType(r.DeviceType)
	if !ValidDeviceTypes[deviceType] {
		deviceType = DeviceUnknown
	}

	// Validate game minute
	if r.GameMinute < 0 || r.GameMinute > 120 {
		return nil, NewValidationError("game_minute", "must be between 0 and 120")
	}

	// Parse timestamp
	timestamp, err := time.Parse(time.RFC3339, r.Timestamp)
	if err != nil {
		// Try RFC3339Nano
		timestamp, err = time.Parse(time.RFC3339Nano, r.Timestamp)
		if err != nil {
			timestamp = time.Now().UTC()
		}
	}

	return &EngagementEvent{
		EventID:            eventUUID,
		MatchID:            r.MatchID,
		UserID:             r.UserID,
		SessionID:          r.SessionID,
		EngagementType:     engagementType,
		EngagementSubtype:  r.EngagementSubtype,
		RelatedGameEventID: relatedGameEventID,
		GameMinute:         r.GameMinute,
		DeviceType:         deviceType,
		Platform:           r.Platform,
		CountryCode:        r.CountryCode,
		Content:            r.Content,
		Metadata:           r.Metadata,
		Timestamp:          timestamp,
	}, nil
}

// MetadataJSON serializes the engagement metadata to a JSON string.
func (e *EngagementEvent) MetadataJSON() string {
	if e.Metadata == nil {
		return "{}"
	}
	data, err := json.Marshal(e.Metadata)
	if err != nil {
		return "{}"
	}
	return string(data)
}

// EngagementKafkaMessage represents the serialized form for Kafka.
type EngagementKafkaMessage struct {
	EventID            string                 `json:"eventId"`
	MatchID            string                 `json:"matchId"`
	UserID             string                 `json:"userId"`
	SessionID          string                 `json:"sessionId"`
	EngagementType     string                 `json:"engagementType"`
	EngagementSubtype  string                 `json:"engagementSubtype"`
	RelatedGameEventID *string                `json:"relatedGameEventId,omitempty"`
	GameMinute         int                    `json:"gameMinute"`
	DeviceType         string                 `json:"deviceType"`
	Platform           string                 `json:"platform"`
	CountryCode        string                 `json:"countryCode"`
	Content            string                 `json:"content"`
	Metadata           map[string]interface{} `json:"metadata,omitempty"`
	Timestamp          string                 `json:"timestamp"`
}

// ToKafkaMessage converts an EngagementEvent to a JSON byte slice for Kafka.
func (e *EngagementEvent) ToKafkaMessage() ([]byte, error) {
	var relatedEventID *string
	if e.RelatedGameEventID != nil {
		id := e.RelatedGameEventID.String()
		relatedEventID = &id
	}

	msg := EngagementKafkaMessage{
		EventID:            e.EventID.String(),
		MatchID:            e.MatchID,
		UserID:             e.UserID,
		SessionID:          e.SessionID,
		EngagementType:     string(e.EngagementType),
		EngagementSubtype:  e.EngagementSubtype,
		RelatedGameEventID: relatedEventID,
		GameMinute:         e.GameMinute,
		DeviceType:         string(e.DeviceType),
		Platform:           e.Platform,
		CountryCode:        e.CountryCode,
		Content:            e.Content,
		Metadata:           e.Metadata,
		Timestamp:          e.Timestamp.Format(time.RFC3339Nano),
	}
	return json.Marshal(msg)
}

// EngagementMetrics represents aggregated engagement metrics for a match.
type EngagementMetrics struct {
	MatchID              string                        `json:"matchId"`
	TotalEngagements     int64                         `json:"totalEngagements"`
	UniqueUsers          int64                         `json:"uniqueUsers"`
	EngagementsByType    map[string]int64              `json:"engagementsByType"`
	EngagementsBySubtype map[string]map[string]int64   `json:"engagementsBySubtype"`
	DeviceBreakdown      map[string]int64              `json:"deviceBreakdown"`
	PlatformBreakdown    map[string]int64              `json:"platformBreakdown"`
	CountryBreakdown     map[string]int64              `json:"countryBreakdown"`
	PeakEngagement       *PeakEngagementMinute         `json:"peakEngagement,omitempty"`
	EngagementTimeline   []EngagementTimelinePoint     `json:"engagementTimeline,omitempty"`
}

// PeakEngagementMinute represents the minute with highest engagement.
type PeakEngagementMinute struct {
	GameMinute       int   `json:"gameMinute"`
	EngagementCount  int64 `json:"engagementCount"`
	UniqueUsers      int64 `json:"uniqueUsers"`
}

// EngagementTimelinePoint represents engagements at a specific game minute.
type EngagementTimelinePoint struct {
	GameMinute  int   `json:"gameMinute"`
	Engagements int64 `json:"engagements"`
	UniqueUsers int64 `json:"uniqueUsers"`
}
