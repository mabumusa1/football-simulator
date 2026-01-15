package domain

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestEventRequest_ToEvent_ValidInput(t *testing.T) {
	eventID := uuid.New().String()
	timestamp := time.Now().Format(time.RFC3339)

	req := &EventRequest{
		EventID:   eventID,
		MatchID:   "match-123",
		EventType: "goal",
		Timestamp: timestamp,
		TeamID:    1,
		PlayerID:  "player-456",
		Metadata:  map[string]interface{}{"minute": 45},
	}

	event, err := req.ToEvent()
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if event.EventID.String() != eventID {
		t.Errorf("expected eventID %s, got %s", eventID, event.EventID.String())
	}
	if event.MatchID != "match-123" {
		t.Errorf("expected matchID 'match-123', got %s", event.MatchID)
	}
	if event.EventType != EventTypeGoal {
		t.Errorf("expected eventType 'goal', got %s", event.EventType)
	}
	if event.TeamID != 1 {
		t.Errorf("expected teamID 1, got %d", event.TeamID)
	}
	if event.PlayerID != "player-456" {
		t.Errorf("expected playerID 'player-456', got %s", event.PlayerID)
	}
}

func TestEventRequest_ToEvent_AllEventTypes(t *testing.T) {
	eventTypes := []struct {
		input    string
		expected EventType
	}{
		{"pass", EventTypePass},
		{"shot", EventTypeShot},
		{"goal", EventTypeGoal},
		{"foul", EventTypeFoul},
		{"yellow_card", EventTypeYellowCard},
		{"red_card", EventTypeRedCard},
		{"substitution", EventTypeSubstitution},
		{"offside", EventTypeOffside},
		{"corner", EventTypeCorner},
		{"free_kick", EventTypeFreeKick},
		{"interception", EventTypeInterception},
	}

	for _, tc := range eventTypes {
		t.Run(tc.input, func(t *testing.T) {
			req := &EventRequest{
				EventID:   uuid.New().String(),
				MatchID:   "match-123",
				EventType: tc.input,
				Timestamp: time.Now().Format(time.RFC3339),
				TeamID:    1,
			}

			event, err := req.ToEvent()
			if err != nil {
				t.Fatalf("expected no error for event type %s, got %v", tc.input, err)
			}

			if event.EventType != tc.expected {
				t.Errorf("expected event type %s, got %s", tc.expected, event.EventType)
			}
		})
	}
}

func TestEventRequest_ToEvent_InvalidEventID(t *testing.T) {
	req := &EventRequest{
		EventID:   "not-a-uuid",
		MatchID:   "match-123",
		EventType: "goal",
		Timestamp: time.Now().Format(time.RFC3339),
		TeamID:    1,
	}

	_, err := req.ToEvent()
	if err == nil {
		t.Fatal("expected validation error for invalid UUID")
	}

	ve := AsValidationError(err)
	if ve == nil {
		t.Fatal("expected ValidationError type")
	}
	if ve.Field != "eventId" {
		t.Errorf("expected field 'eventId', got %s", ve.Field)
	}
}

func TestEventRequest_ToEvent_EmptyMatchID(t *testing.T) {
	req := &EventRequest{
		EventID:   uuid.New().String(),
		MatchID:   "",
		EventType: "goal",
		Timestamp: time.Now().Format(time.RFC3339),
		TeamID:    1,
	}

	_, err := req.ToEvent()
	if err == nil {
		t.Fatal("expected validation error for empty matchId")
	}

	ve := AsValidationError(err)
	if ve == nil {
		t.Fatal("expected ValidationError type")
	}
	if ve.Field != "matchId" {
		t.Errorf("expected field 'matchId', got %s", ve.Field)
	}
}

func TestEventRequest_ToEvent_InvalidEventType(t *testing.T) {
	req := &EventRequest{
		EventID:   uuid.New().String(),
		MatchID:   "match-123",
		EventType: "invalid_type",
		Timestamp: time.Now().Format(time.RFC3339),
		TeamID:    1,
	}

	_, err := req.ToEvent()
	if err == nil {
		t.Fatal("expected validation error for invalid event type")
	}

	ve := AsValidationError(err)
	if ve == nil {
		t.Fatal("expected ValidationError type")
	}
	if ve.Field != "eventType" {
		t.Errorf("expected field 'eventType', got %s", ve.Field)
	}
}

func TestEventRequest_ToEvent_InvalidTimestamp(t *testing.T) {
	req := &EventRequest{
		EventID:   uuid.New().String(),
		MatchID:   "match-123",
		EventType: "goal",
		Timestamp: "not-a-timestamp",
		TeamID:    1,
	}

	_, err := req.ToEvent()
	if err == nil {
		t.Fatal("expected validation error for invalid timestamp")
	}

	ve := AsValidationError(err)
	if ve == nil {
		t.Fatal("expected ValidationError type")
	}
	if ve.Field != "timestamp" {
		t.Errorf("expected field 'timestamp', got %s", ve.Field)
	}
}

func TestEventRequest_ToEvent_InvalidTeamID(t *testing.T) {
	invalidTeamIDs := []int{0, 3, -1, 100}

	for _, teamID := range invalidTeamIDs {
		t.Run("teamID_"+string(rune('0'+teamID)), func(t *testing.T) {
			req := &EventRequest{
				EventID:   uuid.New().String(),
				MatchID:   "match-123",
				EventType: "goal",
				Timestamp: time.Now().Format(time.RFC3339),
				TeamID:    teamID,
			}

			_, err := req.ToEvent()
			if err == nil {
				t.Fatalf("expected validation error for teamID %d", teamID)
			}

			ve := AsValidationError(err)
			if ve == nil {
				t.Fatal("expected ValidationError type")
			}
			if ve.Field != "teamId" {
				t.Errorf("expected field 'teamId', got %s", ve.Field)
			}
		})
	}
}

func TestEventRequest_ToEvent_ValidTeamIDs(t *testing.T) {
	validTeamIDs := []int{1, 2}

	for _, teamID := range validTeamIDs {
		t.Run("teamID_valid", func(t *testing.T) {
			req := &EventRequest{
				EventID:   uuid.New().String(),
				MatchID:   "match-123",
				EventType: "goal",
				Timestamp: time.Now().Format(time.RFC3339),
				TeamID:    teamID,
			}

			event, err := req.ToEvent()
			if err != nil {
				t.Fatalf("expected no error for teamID %d, got %v", teamID, err)
			}

			if event.TeamID != teamID {
				t.Errorf("expected teamID %d, got %d", teamID, event.TeamID)
			}
		})
	}
}

func TestEvent_MetadataJSON_WithData(t *testing.T) {
	event := &Event{
		Metadata: map[string]interface{}{
			"minute":   45,
			"location": "penalty_box",
		},
	}

	jsonStr := event.MetadataJSON()

	var result map[string]interface{}
	err := json.Unmarshal([]byte(jsonStr), &result)
	if err != nil {
		t.Fatalf("failed to unmarshal metadata JSON: %v", err)
	}

	if result["minute"] != float64(45) {
		t.Errorf("expected minute 45, got %v", result["minute"])
	}
	if result["location"] != "penalty_box" {
		t.Errorf("expected location 'penalty_box', got %v", result["location"])
	}
}

func TestEvent_MetadataJSON_NilMetadata(t *testing.T) {
	event := &Event{
		Metadata: nil,
	}

	jsonStr := event.MetadataJSON()

	if jsonStr != "{}" {
		t.Errorf("expected '{}' for nil metadata, got %s", jsonStr)
	}
}

func TestEvent_MetadataJSON_EmptyMetadata(t *testing.T) {
	event := &Event{
		Metadata: map[string]interface{}{},
	}

	jsonStr := event.MetadataJSON()

	if jsonStr != "{}" {
		t.Errorf("expected '{}' for empty metadata, got %s", jsonStr)
	}
}

func TestEvent_ToKafkaMessage(t *testing.T) {
	eventID := uuid.New()
	timestamp := time.Now()

	event := &Event{
		EventID:   eventID,
		MatchID:   "match-123",
		EventType: EventTypeGoal,
		Timestamp: timestamp,
		TeamID:    1,
		PlayerID:  "player-456",
		Metadata:  map[string]interface{}{"minute": 45},
	}

	data, err := event.ToKafkaMessage()
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	var msg KafkaMessage
	err = json.Unmarshal(data, &msg)
	if err != nil {
		t.Fatalf("failed to unmarshal Kafka message: %v", err)
	}

	if msg.EventID != eventID.String() {
		t.Errorf("expected eventID %s, got %s", eventID.String(), msg.EventID)
	}
	if msg.MatchID != "match-123" {
		t.Errorf("expected matchID 'match-123', got %s", msg.MatchID)
	}
	if msg.EventType != "goal" {
		t.Errorf("expected eventType 'goal', got %s", msg.EventType)
	}
	if msg.TeamID != 1 {
		t.Errorf("expected teamID 1, got %d", msg.TeamID)
	}
	if msg.PlayerID != "player-456" {
		t.Errorf("expected playerID 'player-456', got %s", msg.PlayerID)
	}
}

func TestEventFromKafkaMessage_ValidRFC3339Nano(t *testing.T) {
	eventID := uuid.New()
	timestamp := time.Now()

	msg := KafkaMessage{
		EventID:   eventID.String(),
		MatchID:   "match-123",
		EventType: "goal",
		Timestamp: timestamp.Format(time.RFC3339Nano),
		TeamID:    1,
		PlayerID:  "player-456",
		Metadata:  map[string]interface{}{"minute": float64(45)},
	}

	data, err := json.Marshal(msg)
	if err != nil {
		t.Fatalf("failed to marshal test message: %v", err)
	}

	event, err := EventFromKafkaMessage(data)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if event.EventID != eventID {
		t.Errorf("expected eventID %s, got %s", eventID.String(), event.EventID.String())
	}
	if event.MatchID != "match-123" {
		t.Errorf("expected matchID 'match-123', got %s", event.MatchID)
	}
	if event.EventType != EventTypeGoal {
		t.Errorf("expected eventType 'goal', got %s", event.EventType)
	}
	if event.TeamID != 1 {
		t.Errorf("expected teamID 1, got %d", event.TeamID)
	}
}

func TestEventFromKafkaMessage_ValidRFC3339(t *testing.T) {
	eventID := uuid.New()
	timestamp := time.Now()

	msg := KafkaMessage{
		EventID:   eventID.String(),
		MatchID:   "match-123",
		EventType: "shot",
		Timestamp: timestamp.Format(time.RFC3339),
		TeamID:    2,
	}

	data, err := json.Marshal(msg)
	if err != nil {
		t.Fatalf("failed to marshal test message: %v", err)
	}

	event, err := EventFromKafkaMessage(data)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if event.EventType != EventTypeShot {
		t.Errorf("expected eventType 'shot', got %s", event.EventType)
	}
	if event.TeamID != 2 {
		t.Errorf("expected teamID 2, got %d", event.TeamID)
	}
}

func TestEventFromKafkaMessage_InvalidJSON(t *testing.T) {
	_, err := EventFromKafkaMessage([]byte("not valid json"))
	if err == nil {
		t.Fatal("expected error for invalid JSON")
	}
}

func TestEventFromKafkaMessage_InvalidEventID(t *testing.T) {
	msg := KafkaMessage{
		EventID:   "not-a-uuid",
		MatchID:   "match-123",
		EventType: "goal",
		Timestamp: time.Now().Format(time.RFC3339),
		TeamID:    1,
	}

	data, _ := json.Marshal(msg)

	_, err := EventFromKafkaMessage(data)
	if err == nil {
		t.Fatal("expected error for invalid UUID")
	}
}

func TestEventFromKafkaMessage_InvalidTimestamp(t *testing.T) {
	msg := KafkaMessage{
		EventID:   uuid.New().String(),
		MatchID:   "match-123",
		EventType: "goal",
		Timestamp: "not-a-timestamp",
		TeamID:    1,
	}

	data, _ := json.Marshal(msg)

	_, err := EventFromKafkaMessage(data)
	if err == nil {
		t.Fatal("expected error for invalid timestamp")
	}
}

func TestEventFromKafkaMessage_RoundTrip(t *testing.T) {
	eventID := uuid.New()
	timestamp := time.Now().Truncate(time.Nanosecond)

	original := &Event{
		EventID:   eventID,
		MatchID:   "match-123",
		EventType: EventTypeGoal,
		Timestamp: timestamp,
		TeamID:    1,
		PlayerID:  "player-456",
		Metadata:  map[string]interface{}{"minute": float64(45)},
	}

	// Convert to Kafka message
	data, err := original.ToKafkaMessage()
	if err != nil {
		t.Fatalf("failed to convert to Kafka message: %v", err)
	}

	// Convert back from Kafka message
	restored, err := EventFromKafkaMessage(data)
	if err != nil {
		t.Fatalf("failed to restore from Kafka message: %v", err)
	}

	if restored.EventID != original.EventID {
		t.Errorf("eventID mismatch: %s vs %s", original.EventID, restored.EventID)
	}
	if restored.MatchID != original.MatchID {
		t.Errorf("matchID mismatch: %s vs %s", original.MatchID, restored.MatchID)
	}
	if restored.EventType != original.EventType {
		t.Errorf("eventType mismatch: %s vs %s", original.EventType, restored.EventType)
	}
	if restored.TeamID != original.TeamID {
		t.Errorf("teamID mismatch: %d vs %d", original.TeamID, restored.TeamID)
	}
	if restored.PlayerID != original.PlayerID {
		t.Errorf("playerID mismatch: %s vs %s", original.PlayerID, restored.PlayerID)
	}
}

func TestValidEventTypes(t *testing.T) {
	expectedTypes := []EventType{
		EventTypePass,
		EventTypeShot,
		EventTypeGoal,
		EventTypeFoul,
		EventTypeYellowCard,
		EventTypeRedCard,
		EventTypeSubstitution,
		EventTypeOffside,
		EventTypeCorner,
		EventTypeFreeKick,
		EventTypeInterception,
	}

	if len(ValidEventTypes) != len(expectedTypes) {
		t.Errorf("expected %d valid event types, got %d", len(expectedTypes), len(ValidEventTypes))
	}

	for _, et := range expectedTypes {
		if !ValidEventTypes[et] {
			t.Errorf("expected event type %s to be valid", et)
		}
	}
}
