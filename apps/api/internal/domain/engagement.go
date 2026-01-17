package domain

import (
	"time"

	"github.com/google/uuid"

	sharedDomain "github.com/mabumusa1/football-simulator/pkg/domain"
)

// Re-export types from shared domain package
type EngagementType = sharedDomain.EngagementType
type EngagementEvent = sharedDomain.EngagementEvent
type EngagementKafkaMessage = sharedDomain.EngagementKafkaMessage

// Re-export constants from shared domain package
const (
	EngagementTypeReaction    = sharedDomain.EngagementTypeReaction
	EngagementTypeComment     = sharedDomain.EngagementTypeComment
	EngagementTypeVideoAction = sharedDomain.EngagementTypeVideoAction
	EngagementTypeShare       = sharedDomain.EngagementTypeShare
	EngagementTypePrediction  = sharedDomain.EngagementTypePrediction
	EngagementTypeClick       = sharedDomain.EngagementTypeClick
	EngagementTypeSession     = sharedDomain.EngagementTypeSession
)

// Re-export functions from shared domain package
var EngagementEventFromKafkaMessage = sharedDomain.EngagementEventFromKafkaMessage

// Validation error messages
const errRequired = "is required"

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
		return nil, NewValidationError("match_id", errRequired)
	}
	if r.UserID == "" {
		return nil, NewValidationError("user_id", errRequired)
	}
	if r.SessionID == "" {
		return nil, NewValidationError("session_id", errRequired)
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

	// Validate device type - convert to string for shared struct
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
		DeviceType:         string(deviceType),
		Platform:           r.Platform,
		CountryCode:        r.CountryCode,
		Content:            r.Content,
		Metadata:           r.Metadata,
		Timestamp:          timestamp,
	}, nil
}

// EngagementMetrics represents aggregated engagement metrics for a match.
type EngagementMetrics struct {
	MatchID              string                      `json:"matchId"`
	TotalEngagements     int64                       `json:"totalEngagements"`
	UniqueUsers          int64                       `json:"uniqueUsers"`
	EngagementsByType    map[string]int64            `json:"engagementsByType"`
	EngagementsBySubtype map[string]map[string]int64 `json:"engagementsBySubtype"`
	DeviceBreakdown      map[string]int64            `json:"deviceBreakdown"`
	PlatformBreakdown    map[string]int64            `json:"platformBreakdown"`
	CountryBreakdown     map[string]int64            `json:"countryBreakdown"`
	PeakEngagement       *PeakEngagementMinute       `json:"peakEngagement,omitempty"`
	EngagementTimeline   []EngagementTimelinePoint   `json:"engagementTimeline,omitempty"`
}

// PeakEngagementMinute represents the minute with highest engagement.
type PeakEngagementMinute struct {
	GameMinute      int   `json:"gameMinute"`
	EngagementCount int64 `json:"engagementCount"`
	UniqueUsers     int64 `json:"uniqueUsers"`
}

// EngagementTimelinePoint represents engagements at a specific game minute.
type EngagementTimelinePoint struct {
	GameMinute  int   `json:"gameMinute"`
	Engagements int64 `json:"engagements"`
	UniqueUsers int64 `json:"uniqueUsers"`
}
