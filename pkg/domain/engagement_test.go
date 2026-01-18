package domain

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestEngagementEvent_ToKafkaMessage(t *testing.T) {
	relatedEventID := uuid.New()
	event := &EngagementEvent{
		EventID:            uuid.New(),
		MatchID:            "match-123",
		UserID:             "user-456",
		SessionID:          "session-789",
		EngagementType:     EngagementTypeReaction,
		EngagementSubtype:  "cheer",
		RelatedGameEventID: &relatedEventID,
		GameMinute:         45,
		DeviceType:         "mobile",
		Platform:           "ios",
		CountryCode:        "US",
		Content:            "Great goal!",
		Metadata:           map[string]interface{}{"intensity": 10},
		Timestamp:          time.Now().UTC(),
	}

	data, err := event.ToKafkaMessage()
	if err != nil {
		t.Fatalf("ToKafkaMessage() unexpected error: %v", err)
	}

	var msg EngagementKafkaMessage
	if err := json.Unmarshal(data, &msg); err != nil {
		t.Fatalf("ToKafkaMessage() returned invalid JSON: %v", err)
	}

	if msg.EventID != event.EventID.String() {
		t.Errorf("EventID = %q, want %q", msg.EventID, event.EventID.String())
	}
	if msg.MatchID != event.MatchID {
		t.Errorf("MatchID = %q, want %q", msg.MatchID, event.MatchID)
	}
	if msg.UserID != event.UserID {
		t.Errorf("UserID = %q, want %q", msg.UserID, event.UserID)
	}
	if msg.EngagementType != string(event.EngagementType) {
		t.Errorf("EngagementType = %q, want %q", msg.EngagementType, event.EngagementType)
	}
}

func TestEngagementEvent_ToKafkaMessage_NilRelatedEvent(t *testing.T) {
	event := &EngagementEvent{
		EventID:            uuid.New(),
		MatchID:            "match-123",
		UserID:             "user-456",
		SessionID:          "session-789",
		EngagementType:     EngagementTypeSession,
		EngagementSubtype:  "join",
		RelatedGameEventID: nil,
		GameMinute:         0,
		DeviceType:         "mobile",
		Timestamp:          time.Now().UTC(),
	}

	data, err := event.ToKafkaMessage()
	if err != nil {
		t.Fatalf("ToKafkaMessage() unexpected error: %v", err)
	}

	var msg EngagementKafkaMessage
	if err := json.Unmarshal(data, &msg); err != nil {
		t.Fatalf("ToKafkaMessage() returned invalid JSON: %v", err)
	}

	if msg.RelatedGameEventID != nil {
		t.Errorf("RelatedGameEventID should be nil, got %v", msg.RelatedGameEventID)
	}
}

func TestEngagementEventFromKafkaMessage(t *testing.T) {
	originalEvent := &EngagementEvent{
		EventID:           uuid.New(),
		MatchID:           "match-123",
		UserID:            "user-456",
		SessionID:         "session-789",
		EngagementType:    EngagementTypeReaction,
		EngagementSubtype: "cheer",
		GameMinute:        45,
		DeviceType:        "mobile",
		Platform:          "ios",
		CountryCode:       "US",
		Content:           "Great goal!",
		Timestamp:         time.Now().UTC(),
	}

	data, err := originalEvent.ToKafkaMessage()
	if err != nil {
		t.Fatalf("ToKafkaMessage() error: %v", err)
	}

	parsedEvent, err := EngagementEventFromKafkaMessage(data)
	if err != nil {
		t.Fatalf("EngagementEventFromKafkaMessage() error: %v", err)
	}

	if parsedEvent.EventID != originalEvent.EventID {
		t.Errorf("EventID = %v, want %v", parsedEvent.EventID, originalEvent.EventID)
	}
	if parsedEvent.MatchID != originalEvent.MatchID {
		t.Errorf("MatchID = %q, want %q", parsedEvent.MatchID, originalEvent.MatchID)
	}
	if parsedEvent.UserID != originalEvent.UserID {
		t.Errorf("UserID = %q, want %q", parsedEvent.UserID, originalEvent.UserID)
	}
	if parsedEvent.EngagementType != originalEvent.EngagementType {
		t.Errorf("EngagementType = %q, want %q", parsedEvent.EngagementType, originalEvent.EngagementType)
	}
}

func TestEngagementEventFromKafkaMessage_InvalidJSON(t *testing.T) {
	_, err := EngagementEventFromKafkaMessage([]byte(`{invalid json}`))
	if err == nil {
		t.Error("expected error for invalid JSON")
	}
}

func TestEngagementEventFromKafkaMessage_InvalidEventID(t *testing.T) {
	data := []byte(`{"eventId":"invalid-uuid","matchId":"match-123","userId":"user-456","sessionId":"session-789","engagementType":"reaction","timestamp":"2024-01-01T00:00:00Z"}`)
	_, err := EngagementEventFromKafkaMessage(data)
	if err == nil {
		t.Error("expected error for invalid eventId")
	}
}

func TestEngagementEventFromKafkaMessage_MissingMatchID(t *testing.T) {
	validUUID := uuid.New().String()
	data := []byte(`{"eventId":"` + validUUID + `","matchId":"","userId":"user-456","sessionId":"session-789","engagementType":"reaction","timestamp":"2024-01-01T00:00:00Z"}`)
	_, err := EngagementEventFromKafkaMessage(data)
	if err == nil {
		t.Error("expected error for missing matchId")
	}
}

func TestEngagementEventFromKafkaMessage_MissingUserID(t *testing.T) {
	validUUID := uuid.New().String()
	data := []byte(`{"eventId":"` + validUUID + `","matchId":"match-123","userId":"","sessionId":"session-789","engagementType":"reaction","timestamp":"2024-01-01T00:00:00Z"}`)
	_, err := EngagementEventFromKafkaMessage(data)
	if err == nil {
		t.Error("expected error for missing userId")
	}
}

func TestEngagementEventFromKafkaMessage_WithRelatedEvent(t *testing.T) {
	eventID := uuid.New()
	relatedID := uuid.New()

	msg := EngagementKafkaMessage{
		EventID:            eventID.String(),
		MatchID:            "match-123",
		UserID:             "user-456",
		SessionID:          "session-789",
		EngagementType:     "reaction",
		RelatedGameEventID: stringPtr(relatedID.String()),
		Timestamp:          time.Now().UTC().Format(time.RFC3339Nano),
	}

	data, _ := json.Marshal(msg)
	event, err := EngagementEventFromKafkaMessage(data)
	if err != nil {
		t.Fatalf("EngagementEventFromKafkaMessage() error: %v", err)
	}

	if event.RelatedGameEventID == nil {
		t.Fatal("RelatedGameEventID should not be nil")
	}
	if *event.RelatedGameEventID != relatedID {
		t.Errorf("RelatedGameEventID = %v, want %v", *event.RelatedGameEventID, relatedID)
	}
}

func TestEngagementEvent_MetadataJSON(t *testing.T) {
	t.Run("nil metadata returns empty object", func(t *testing.T) {
		event := &EngagementEvent{
			Metadata: nil,
		}
		json := event.MetadataJSON()
		if json != "{}" {
			t.Errorf("expected {}, got %s", json)
		}
	})

	t.Run("valid metadata returns JSON", func(t *testing.T) {
		event := &EngagementEvent{
			Metadata: map[string]interface{}{"key": "value"},
		}
		json := event.MetadataJSON()
		if json != `{"key":"value"}` {
			t.Errorf("expected {\"key\":\"value\"}, got %s", json)
		}
	})

	t.Run("complex metadata", func(t *testing.T) {
		event := &EngagementEvent{
			Metadata: map[string]interface{}{
				"intensity": 10,
				"location":  "stadium",
			},
		}
		json := event.MetadataJSON()
		if json == "{}" {
			t.Error("expected non-empty JSON")
		}
	})
}

func TestEngagementEvent_GetEventID(t *testing.T) {
	eventID := uuid.New()
	event := &EngagementEvent{
		EventID: eventID,
	}
	if event.GetEventID() != eventID {
		t.Errorf("GetEventID() = %v, want %v", event.GetEventID(), eventID)
	}
}

func TestEngagementType_Constants(t *testing.T) {
	// Verify all engagement type constants are defined
	types := []EngagementType{
		EngagementTypeReaction,
		EngagementTypeComment,
		EngagementTypeVideoAction,
		EngagementTypeShare,
		EngagementTypePrediction,
		EngagementTypeClick,
		EngagementTypeSession,
	}

	for _, engType := range types {
		if engType == "" {
			t.Errorf("EngagementType constant is empty")
		}
	}
}

func stringPtr(s string) *string {
	return &s
}
