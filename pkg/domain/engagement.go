package domain

import (
	"encoding/json"
	"fmt"
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

// EngagementEvent represents a user engagement event.
type EngagementEvent struct {
	EventID            uuid.UUID
	MatchID            string
	UserID             string
	SessionID          string
	EngagementType     EngagementType
	EngagementSubtype  string
	RelatedGameEventID *uuid.UUID
	GameMinute         int
	DeviceType         string
	Platform           string
	CountryCode        string
	Content            string
	Metadata           map[string]interface{}
	Timestamp          time.Time
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

// GetEventID returns the event's UUID for the EventWithID interface.
func (e *EngagementEvent) GetEventID() uuid.UUID {
	return e.EventID
}

// MetadataJSON serializes the engagement metadata to a JSON string.
// Returns an empty JSON object "{}" if metadata is nil or serialization fails.
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
		DeviceType:         e.DeviceType,
		Platform:           e.Platform,
		CountryCode:        e.CountryCode,
		Content:            e.Content,
		Metadata:           e.Metadata,
		Timestamp:          e.Timestamp.Format(time.RFC3339Nano),
	}
	return json.Marshal(msg)
}

// EngagementEventFromKafkaMessage parses a Kafka message into an EngagementEvent.
func EngagementEventFromKafkaMessage(data []byte) (*EngagementEvent, error) {
	var msg EngagementKafkaMessage
	if err := json.Unmarshal(data, &msg); err != nil {
		return nil, fmt.Errorf("failed to unmarshal engagement JSON: %w", err)
	}

	// Parse event UUID
	eventID, err := uuid.Parse(msg.EventID)
	if err != nil {
		return nil, fmt.Errorf("invalid engagement eventId UUID: %w", err)
	}

	// Validate required fields
	if msg.MatchID == "" {
		return nil, fmt.Errorf("engagement matchId is required")
	}
	if msg.UserID == "" {
		return nil, fmt.Errorf("engagement userId is required")
	}

	// Parse related game event ID if present
	var relatedGameEventID *uuid.UUID
	if msg.RelatedGameEventID != nil && *msg.RelatedGameEventID != "" {
		parsed, err := uuid.Parse(*msg.RelatedGameEventID)
		if err == nil {
			relatedGameEventID = &parsed
		}
	}

	// Parse timestamp
	timestamp, err := time.Parse(time.RFC3339Nano, msg.Timestamp)
	if err != nil {
		timestamp, err = time.Parse(time.RFC3339, msg.Timestamp)
		if err != nil {
			timestamp = time.Now().UTC()
		}
	}

	return &EngagementEvent{
		EventID:            eventID,
		MatchID:            msg.MatchID,
		UserID:             msg.UserID,
		SessionID:          msg.SessionID,
		EngagementType:     EngagementType(msg.EngagementType),
		EngagementSubtype:  msg.EngagementSubtype,
		RelatedGameEventID: relatedGameEventID,
		GameMinute:         msg.GameMinute,
		DeviceType:         msg.DeviceType,
		Platform:           msg.Platform,
		CountryCode:        msg.CountryCode,
		Content:            msg.Content,
		Metadata:           msg.Metadata,
		Timestamp:          timestamp,
	}, nil
}
