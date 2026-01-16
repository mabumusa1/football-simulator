package domain

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestEngagementType_Constants(t *testing.T) {
	// Test that all engagement type constants are defined correctly
	testCases := []struct {
		name     string
		got      EngagementType
		expected string
	}{
		{"Reaction", EngagementTypeReaction, "reaction"},
		{"Comment", EngagementTypeComment, "comment"},
		{"VideoAction", EngagementTypeVideoAction, "video_action"},
		{"Share", EngagementTypeShare, "share"},
		{"Prediction", EngagementTypePrediction, "prediction"},
		{"Click", EngagementTypeClick, "click"},
		{"Session", EngagementTypeSession, "session"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if string(tc.got) != tc.expected {
				t.Errorf("%s = %q, want %q", tc.name, tc.got, tc.expected)
			}
		})
	}
}

func TestEngagementEventFromKafkaMessage_ValidMessage(t *testing.T) {
	eventID := uuid.New()
	relatedGameEventID := uuid.New()
	timestamp := time.Now().UTC().Truncate(time.Second)

	msg := EngagementKafkaMessage{
		EventID:            eventID.String(),
		MatchID:            "match-123",
		UserID:             "user-456",
		SessionID:          "session-789",
		EngagementType:     "reaction",
		EngagementSubtype:  "like",
		RelatedGameEventID: strPtr(relatedGameEventID.String()),
		GameMinute:         45,
		DeviceType:         "mobile",
		Platform:           "ios",
		CountryCode:        "US",
		Content:            "test content",
		Metadata:           map[string]interface{}{"key": "value"},
		Timestamp:          timestamp.Format(time.RFC3339Nano),
	}

	data, err := json.Marshal(msg)
	if err != nil {
		t.Fatalf("Failed to marshal test message: %v", err)
	}

	event, err := EngagementEventFromKafkaMessage(data)

	if err != nil {
		t.Fatalf("EngagementEventFromKafkaMessage returned error: %v", err)
	}
	if event == nil {
		t.Fatal("EngagementEventFromKafkaMessage returned nil")
	}
	if event.EventID != eventID {
		t.Errorf("EventID = %v, want %v", event.EventID, eventID)
	}
	if event.MatchID != "match-123" {
		t.Errorf("MatchID = %q, want %q", event.MatchID, "match-123")
	}
	if event.UserID != "user-456" {
		t.Errorf("UserID = %q, want %q", event.UserID, "user-456")
	}
	if event.SessionID != "session-789" {
		t.Errorf("SessionID = %q, want %q", event.SessionID, "session-789")
	}
	if event.EngagementType != EngagementTypeReaction {
		t.Errorf("EngagementType = %q, want %q", event.EngagementType, EngagementTypeReaction)
	}
	if event.EngagementSubtype != "like" {
		t.Errorf("EngagementSubtype = %q, want %q", event.EngagementSubtype, "like")
	}
	if event.RelatedGameEventID == nil {
		t.Error("RelatedGameEventID should not be nil")
	} else if *event.RelatedGameEventID != relatedGameEventID {
		t.Errorf("RelatedGameEventID = %v, want %v", *event.RelatedGameEventID, relatedGameEventID)
	}
	if event.GameMinute != 45 {
		t.Errorf("GameMinute = %d, want %d", event.GameMinute, 45)
	}
	if event.DeviceType != "mobile" {
		t.Errorf("DeviceType = %q, want %q", event.DeviceType, "mobile")
	}
	if event.Platform != "ios" {
		t.Errorf("Platform = %q, want %q", event.Platform, "ios")
	}
	if event.CountryCode != "US" {
		t.Errorf("CountryCode = %q, want %q", event.CountryCode, "US")
	}
	if event.Content != "test content" {
		t.Errorf("Content = %q, want %q", event.Content, "test content")
	}
	if event.Metadata["key"] != "value" {
		t.Errorf("Metadata[key] = %v, want %v", event.Metadata["key"], "value")
	}
}

func TestEngagementEventFromKafkaMessage_InvalidJSON(t *testing.T) {
	_, err := EngagementEventFromKafkaMessage([]byte("invalid json"))

	if err == nil {
		t.Error("EngagementEventFromKafkaMessage should return error for invalid JSON")
	}
}

func TestEngagementEventFromKafkaMessage_InvalidEventID(t *testing.T) {
	msg := EngagementKafkaMessage{
		EventID: "not-a-uuid",
		MatchID: "match-123",
		UserID:  "user-456",
	}

	data, _ := json.Marshal(msg)
	_, err := EngagementEventFromKafkaMessage(data)

	if err == nil {
		t.Error("EngagementEventFromKafkaMessage should return error for invalid EventID")
	}
}

func TestEngagementEventFromKafkaMessage_MissingMatchID(t *testing.T) {
	msg := EngagementKafkaMessage{
		EventID: uuid.New().String(),
		MatchID: "",
		UserID:  "user-456",
	}

	data, _ := json.Marshal(msg)
	_, err := EngagementEventFromKafkaMessage(data)

	if err == nil {
		t.Error("EngagementEventFromKafkaMessage should return error for missing MatchID")
	}
}

func TestEngagementEventFromKafkaMessage_MissingUserID(t *testing.T) {
	msg := EngagementKafkaMessage{
		EventID: uuid.New().String(),
		MatchID: "match-123",
		UserID:  "",
	}

	data, _ := json.Marshal(msg)
	_, err := EngagementEventFromKafkaMessage(data)

	if err == nil {
		t.Error("EngagementEventFromKafkaMessage should return error for missing UserID")
	}
}

func TestEngagementEventFromKafkaMessage_InvalidRelatedGameEventID(t *testing.T) {
	msg := EngagementKafkaMessage{
		EventID:            uuid.New().String(),
		MatchID:            "match-123",
		UserID:             "user-456",
		RelatedGameEventID: strPtr("not-a-uuid"),
		Timestamp:          time.Now().Format(time.RFC3339),
	}

	data, _ := json.Marshal(msg)
	event, err := EngagementEventFromKafkaMessage(data)

	// Invalid related game event ID should be silently ignored (set to nil)
	if err != nil {
		t.Errorf("EngagementEventFromKafkaMessage should not error for invalid RelatedGameEventID: %v", err)
	}
	if event.RelatedGameEventID != nil {
		t.Error("Invalid RelatedGameEventID should result in nil")
	}
}

func TestEngagementEventFromKafkaMessage_NilRelatedGameEventID(t *testing.T) {
	msg := EngagementKafkaMessage{
		EventID:            uuid.New().String(),
		MatchID:            "match-123",
		UserID:             "user-456",
		RelatedGameEventID: nil,
		Timestamp:          time.Now().Format(time.RFC3339),
	}

	data, _ := json.Marshal(msg)
	event, err := EngagementEventFromKafkaMessage(data)

	if err != nil {
		t.Errorf("EngagementEventFromKafkaMessage returned error: %v", err)
	}
	if event.RelatedGameEventID != nil {
		t.Error("Nil RelatedGameEventID should remain nil")
	}
}

func TestEngagementEventFromKafkaMessage_EmptyRelatedGameEventID(t *testing.T) {
	msg := EngagementKafkaMessage{
		EventID:            uuid.New().String(),
		MatchID:            "match-123",
		UserID:             "user-456",
		RelatedGameEventID: strPtr(""),
		Timestamp:          time.Now().Format(time.RFC3339),
	}

	data, _ := json.Marshal(msg)
	event, err := EngagementEventFromKafkaMessage(data)

	if err != nil {
		t.Errorf("EngagementEventFromKafkaMessage returned error: %v", err)
	}
	if event.RelatedGameEventID != nil {
		t.Error("Empty RelatedGameEventID should result in nil")
	}
}

func TestEngagementEventFromKafkaMessage_TimestampParsing(t *testing.T) {
	testCases := []struct {
		name      string
		timestamp string
		valid     bool
	}{
		{"RFC3339Nano", time.Now().Format(time.RFC3339Nano), true},
		{"RFC3339", time.Now().Format(time.RFC3339), true},
		{"Invalid", "not-a-timestamp", true}, // Falls back to current time
		{"Empty", "", true},                  // Falls back to current time
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			msg := EngagementKafkaMessage{
				EventID:   uuid.New().String(),
				MatchID:   "match-123",
				UserID:    "user-456",
				Timestamp: tc.timestamp,
			}

			data, _ := json.Marshal(msg)
			event, err := EngagementEventFromKafkaMessage(data)

			if err != nil {
				t.Errorf("EngagementEventFromKafkaMessage returned error: %v", err)
			}
			if event.Timestamp.IsZero() {
				t.Error("Timestamp should not be zero")
			}
		})
	}
}

func TestEngagementEvent_MetadataJSON_WithData(t *testing.T) {
	event := &EngagementEvent{
		Metadata: map[string]interface{}{
			"key1": "value1",
			"key2": 123,
			"key3": true,
		},
	}

	jsonStr := event.MetadataJSON()

	// Parse the JSON to verify it's valid
	var parsed map[string]interface{}
	if err := json.Unmarshal([]byte(jsonStr), &parsed); err != nil {
		t.Fatalf("MetadataJSON produced invalid JSON: %v", err)
	}

	if parsed["key1"] != "value1" {
		t.Errorf("MetadataJSON key1 = %v, want %v", parsed["key1"], "value1")
	}
	// JSON numbers are float64
	if parsed["key2"].(float64) != 123 {
		t.Errorf("MetadataJSON key2 = %v, want %v", parsed["key2"], 123)
	}
	if parsed["key3"] != true {
		t.Errorf("MetadataJSON key3 = %v, want %v", parsed["key3"], true)
	}
}

func TestEngagementEvent_MetadataJSON_NilMetadata(t *testing.T) {
	event := &EngagementEvent{
		Metadata: nil,
	}

	jsonStr := event.MetadataJSON()

	if jsonStr != "{}" {
		t.Errorf("MetadataJSON with nil = %q, want %q", jsonStr, "{}")
	}
}

func TestEngagementEvent_MetadataJSON_EmptyMetadata(t *testing.T) {
	event := &EngagementEvent{
		Metadata: make(map[string]interface{}),
	}

	jsonStr := event.MetadataJSON()

	if jsonStr != "{}" {
		t.Errorf("MetadataJSON with empty map = %q, want %q", jsonStr, "{}")
	}
}

func TestEngagementEvent_MetadataJSON_NestedData(t *testing.T) {
	event := &EngagementEvent{
		Metadata: map[string]interface{}{
			"outer": map[string]interface{}{
				"inner": "value",
			},
		},
	}

	jsonStr := event.MetadataJSON()

	var parsed map[string]interface{}
	if err := json.Unmarshal([]byte(jsonStr), &parsed); err != nil {
		t.Fatalf("MetadataJSON produced invalid JSON: %v", err)
	}

	outer, ok := parsed["outer"].(map[string]interface{})
	if !ok {
		t.Fatal("MetadataJSON outer is not a map")
	}
	if outer["inner"] != "value" {
		t.Errorf("MetadataJSON outer.inner = %v, want %v", outer["inner"], "value")
	}
}

func TestEngagementKafkaMessage_JSONMarshaling(t *testing.T) {
	eventID := uuid.New().String()
	msg := EngagementKafkaMessage{
		EventID:           eventID,
		MatchID:           "match-123",
		UserID:            "user-456",
		SessionID:         "session-789",
		EngagementType:    "comment",
		EngagementSubtype: "reply",
		GameMinute:        30,
		DeviceType:        "desktop",
		Platform:          "web",
		CountryCode:       "UK",
		Content:           "Great goal!",
		Metadata:          map[string]interface{}{"rating": 5},
		Timestamp:         "2024-01-01T10:00:00Z",
	}

	data, err := json.Marshal(msg)
	if err != nil {
		t.Fatalf("Failed to marshal EngagementKafkaMessage: %v", err)
	}

	var parsed EngagementKafkaMessage
	if err := json.Unmarshal(data, &parsed); err != nil {
		t.Fatalf("Failed to unmarshal EngagementKafkaMessage: %v", err)
	}

	if parsed.EventID != eventID {
		t.Errorf("EventID = %q, want %q", parsed.EventID, eventID)
	}
	if parsed.MatchID != "match-123" {
		t.Errorf("MatchID = %q, want %q", parsed.MatchID, "match-123")
	}
	if parsed.EngagementType != "comment" {
		t.Errorf("EngagementType = %q, want %q", parsed.EngagementType, "comment")
	}
}

func TestEngagementEvent_Struct(t *testing.T) {
	eventID := uuid.New()
	relatedID := uuid.New()
	timestamp := time.Now()

	event := EngagementEvent{
		EventID:            eventID,
		MatchID:            "match-1",
		UserID:             "user-1",
		SessionID:          "session-1",
		EngagementType:     EngagementTypeComment,
		EngagementSubtype:  "reply",
		RelatedGameEventID: &relatedID,
		GameMinute:         60,
		DeviceType:         "tablet",
		Platform:           "android",
		CountryCode:        "DE",
		Content:            "Nice!",
		Metadata:           map[string]interface{}{"flag": true},
		Timestamp:          timestamp,
	}

	if event.EventID != eventID {
		t.Error("EngagementEvent.EventID not set correctly")
	}
	if event.MatchID != "match-1" {
		t.Error("EngagementEvent.MatchID not set correctly")
	}
	if event.EngagementType != EngagementTypeComment {
		t.Error("EngagementEvent.EngagementType not set correctly")
	}
	if event.RelatedGameEventID == nil || *event.RelatedGameEventID != relatedID {
		t.Error("EngagementEvent.RelatedGameEventID not set correctly")
	}
}

// Helper function
func strPtr(s string) *string {
	return &s
}
