package domain

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestEventRequest_ToEvent_ValidRequest(t *testing.T) {
	validUUID := uuid.New().String()
	validTimestamp := time.Now().UTC().Format(time.RFC3339)

	tests := []struct {
		name      string
		request   EventRequest
		wantMatch string
		wantTeam  int
		wantType  EventType
	}{
		{
			name: "valid pass event",
			request: EventRequest{
				EventID:   validUUID,
				MatchID:   "match-123",
				EventType: "pass",
				Timestamp: validTimestamp,
				TeamID:    1,
				PlayerID:  "player-1",
			},
			wantMatch: "match-123",
			wantTeam:  1,
			wantType:  EventTypePass,
		},
		{
			name: "valid goal event team 2",
			request: EventRequest{
				EventID:   validUUID,
				MatchID:   "match-456",
				EventType: "goal",
				Timestamp: validTimestamp,
				TeamID:    2,
				PlayerID:  "player-10",
			},
			wantMatch: "match-456",
			wantTeam:  2,
			wantType:  EventTypeGoal,
		},
		{
			name: "valid event with metadata",
			request: EventRequest{
				EventID:   validUUID,
				MatchID:   "match-789",
				EventType: "shot",
				Timestamp: validTimestamp,
				TeamID:    1,
				PlayerID:  "player-7",
				Metadata:  map[string]interface{}{"on_target": true},
			},
			wantMatch: "match-789",
			wantTeam:  1,
			wantType:  EventTypeShot,
		},
		{
			name: "valid event without player ID",
			request: EventRequest{
				EventID:   validUUID,
				MatchID:   "match-000",
				EventType: "corner",
				Timestamp: validTimestamp,
				TeamID:    2,
				PlayerID:  "",
			},
			wantMatch: "match-000",
			wantTeam:  2,
			wantType:  EventTypeCorner,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			event, err := tt.request.ToEvent()
			if err != nil {
				t.Fatalf("ToEvent() unexpected error: %v", err)
			}
			if event == nil {
				t.Fatal("ToEvent() returned nil event")
			}
			if event.MatchID != tt.wantMatch {
				t.Errorf("ToEvent().MatchID = %q, want %q", event.MatchID, tt.wantMatch)
			}
			if event.TeamID != tt.wantTeam {
				t.Errorf("ToEvent().TeamID = %d, want %d", event.TeamID, tt.wantTeam)
			}
			if event.EventType != tt.wantType {
				t.Errorf("ToEvent().EventType = %q, want %q", event.EventType, tt.wantType)
			}
		})
	}
}

func TestEventRequest_ToEvent_InvalidUUID(t *testing.T) {
	tests := []struct {
		name    string
		eventID string
	}{
		{
			name:    "empty UUID",
			eventID: "",
		},
		{
			name:    "invalid UUID format",
			eventID: "not-a-uuid",
		},
		{
			name:    "partial UUID",
			eventID: "550e8400-e29b-41d4",
		},
		{
			name:    "UUID with spaces",
			eventID: "550e8400 e29b 41d4 a716 446655440000",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := EventRequest{
				EventID:   tt.eventID,
				MatchID:   "match-123",
				EventType: "pass",
				Timestamp: time.Now().UTC().Format(time.RFC3339),
				TeamID:    1,
			}
			event, err := req.ToEvent()
			if err == nil {
				t.Error("ToEvent() expected error for invalid UUID, got nil")
			}
			if event != nil {
				t.Error("ToEvent() expected nil event for invalid UUID")
			}
			ve := AsValidationError(err)
			if ve == nil {
				t.Error("ToEvent() expected ValidationError")
			} else if ve.Field != "eventId" {
				t.Errorf("ValidationError.Field = %q, want %q", ve.Field, "eventId")
			}
		})
	}
}

func TestEventRequest_ToEvent_InvalidMatchID(t *testing.T) {
	req := EventRequest{
		EventID:   uuid.New().String(),
		MatchID:   "", // Empty matchID
		EventType: "pass",
		Timestamp: time.Now().UTC().Format(time.RFC3339),
		TeamID:    1,
	}

	event, err := req.ToEvent()
	if err == nil {
		t.Error("ToEvent() expected error for empty matchId, got nil")
	}
	if event != nil {
		t.Error("ToEvent() expected nil event for empty matchId")
	}
	ve := AsValidationError(err)
	if ve == nil {
		t.Error("ToEvent() expected ValidationError")
	} else if ve.Field != "matchId" {
		t.Errorf("ValidationError.Field = %q, want %q", ve.Field, "matchId")
	}
}

func TestEventRequest_ToEvent_InvalidEventType(t *testing.T) {
	tests := []struct {
		name      string
		eventType string
	}{
		{
			name:      "empty event type",
			eventType: "",
		},
		{
			name:      "invalid event type",
			eventType: "invalid_type",
		},
		{
			name:      "typo in event type",
			eventType: "gol", // typo for "goal"
		},
		{
			name:      "uppercase event type",
			eventType: "PASS", // should be lowercase
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := EventRequest{
				EventID:   uuid.New().String(),
				MatchID:   "match-123",
				EventType: tt.eventType,
				Timestamp: time.Now().UTC().Format(time.RFC3339),
				TeamID:    1,
			}
			event, err := req.ToEvent()
			if err == nil {
				t.Error("ToEvent() expected error for invalid eventType, got nil")
			}
			if event != nil {
				t.Error("ToEvent() expected nil event for invalid eventType")
			}
			ve := AsValidationError(err)
			if ve == nil {
				t.Error("ToEvent() expected ValidationError")
			} else if ve.Field != "eventType" {
				t.Errorf("ValidationError.Field = %q, want %q", ve.Field, "eventType")
			}
		})
	}
}

func TestEventRequest_ToEvent_InvalidTimestamp(t *testing.T) {
	tests := []struct {
		name      string
		timestamp string
	}{
		{
			name:      "empty timestamp",
			timestamp: "",
		},
		{
			name:      "invalid format",
			timestamp: "2024-01-01 12:00:00",
		},
		{
			name:      "date only",
			timestamp: "2024-01-01",
		},
		{
			name:      "unix timestamp",
			timestamp: "1704067200",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := EventRequest{
				EventID:   uuid.New().String(),
				MatchID:   "match-123",
				EventType: "pass",
				Timestamp: tt.timestamp,
				TeamID:    1,
			}
			event, err := req.ToEvent()
			if err == nil {
				t.Error("ToEvent() expected error for invalid timestamp, got nil")
			}
			if event != nil {
				t.Error("ToEvent() expected nil event for invalid timestamp")
			}
			ve := AsValidationError(err)
			if ve == nil {
				t.Error("ToEvent() expected ValidationError")
			} else if ve.Field != "timestamp" {
				t.Errorf("ValidationError.Field = %q, want %q", ve.Field, "timestamp")
			}
		})
	}
}

func TestEventRequest_ToEvent_InvalidTeamID(t *testing.T) {
	tests := []struct {
		name   string
		teamID int
	}{
		{
			name:   "teamID 0",
			teamID: 0,
		},
		{
			name:   "teamID 3",
			teamID: 3,
		},
		{
			name:   "negative teamID",
			teamID: -1,
		},
		{
			name:   "large teamID",
			teamID: 100,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := EventRequest{
				EventID:   uuid.New().String(),
				MatchID:   "match-123",
				EventType: "pass",
				Timestamp: time.Now().UTC().Format(time.RFC3339),
				TeamID:    tt.teamID,
			}
			event, err := req.ToEvent()
			if err == nil {
				t.Errorf("ToEvent() expected error for teamID %d, got nil", tt.teamID)
			}
			if event != nil {
				t.Error("ToEvent() expected nil event for invalid teamID")
			}
			ve := AsValidationError(err)
			if ve == nil {
				t.Error("ToEvent() expected ValidationError")
			} else if ve.Field != "teamId" {
				t.Errorf("ValidationError.Field = %q, want %q", ve.Field, "teamId")
			}
		})
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

	for _, et := range expectedTypes {
		if !ValidEventTypes[et] {
			t.Errorf("EventType %q should be valid", et)
		}
	}

	if len(ValidEventTypes) != len(expectedTypes) {
		t.Errorf("ValidEventTypes has %d entries, want %d", len(ValidEventTypes), len(expectedTypes))
	}
}

func TestEvent_MetadataJSON(t *testing.T) {
	tests := []struct {
		name     string
		metadata map[string]interface{}
		want     string
	}{
		{
			name:     "nil metadata returns empty object",
			metadata: nil,
			want:     "{}",
		},
		{
			name:     "empty metadata returns empty object",
			metadata: map[string]interface{}{},
			want:     "{}",
		},
		{
			name:     "single key metadata",
			metadata: map[string]interface{}{"on_target": true},
			want:     `{"on_target":true}`,
		},
		{
			name:     "multiple keys metadata",
			metadata: map[string]interface{}{"x": 50, "y": 30},
			want:     "", // Order not guaranteed, will check via unmarshaling
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			event := &Event{
				EventID:  uuid.New(),
				Metadata: tt.metadata,
			}
			got := event.MetadataJSON()

			if tt.want != "" {
				if got != tt.want {
					t.Errorf("MetadataJSON() = %q, want %q", got, tt.want)
				}
			} else {
				// For multiple keys, verify it's valid JSON with correct values
				var parsed map[string]interface{}
				if err := json.Unmarshal([]byte(got), &parsed); err != nil {
					t.Errorf("MetadataJSON() returned invalid JSON: %v", err)
				}
			}
		})
	}
}

func TestEvent_ToKafkaMessage(t *testing.T) {
	eventID := uuid.New()
	timestamp := time.Now().UTC()

	event := &Event{
		EventID:   eventID,
		MatchID:   "match-123",
		EventType: EventTypeGoal,
		Timestamp: timestamp,
		TeamID:    1,
		PlayerID:  "player-10",
		Metadata:  map[string]interface{}{"minute": 45},
	}

	data, err := event.ToKafkaMessage()
	if err != nil {
		t.Fatalf("ToKafkaMessage() unexpected error: %v", err)
	}

	var msg KafkaMessage
	if err := json.Unmarshal(data, &msg); err != nil {
		t.Fatalf("ToKafkaMessage() returned invalid JSON: %v", err)
	}

	if msg.EventID != eventID.String() {
		t.Errorf("KafkaMessage.EventID = %q, want %q", msg.EventID, eventID.String())
	}
	if msg.MatchID != "match-123" {
		t.Errorf("KafkaMessage.MatchID = %q, want %q", msg.MatchID, "match-123")
	}
	if msg.EventType != "goal" {
		t.Errorf("KafkaMessage.EventType = %q, want %q", msg.EventType, "goal")
	}
	if msg.TeamID != 1 {
		t.Errorf("KafkaMessage.TeamID = %d, want %d", msg.TeamID, 1)
	}
	if msg.PlayerID != "player-10" {
		t.Errorf("KafkaMessage.PlayerID = %q, want %q", msg.PlayerID, "player-10")
	}
}

func TestEventFromKafkaMessage(t *testing.T) {
	eventID := uuid.New()
	timestamp := time.Now().UTC()

	tests := []struct {
		name    string
		data    []byte
		wantErr bool
	}{
		{
			name: "valid RFC3339Nano timestamp",
			data: []byte(`{
				"eventId": "` + eventID.String() + `",
				"matchId": "match-123",
				"eventType": "goal",
				"timestamp": "` + timestamp.Format(time.RFC3339Nano) + `",
				"teamId": 1,
				"playerId": "player-10"
			}`),
			wantErr: false,
		},
		{
			name: "valid RFC3339 timestamp fallback",
			data: []byte(`{
				"eventId": "` + eventID.String() + `",
				"matchId": "match-123",
				"eventType": "pass",
				"timestamp": "` + timestamp.Format(time.RFC3339) + `",
				"teamId": 2,
				"playerId": "player-5"
			}`),
			wantErr: false,
		},
		{
			name:    "invalid JSON",
			data:    []byte(`{invalid json}`),
			wantErr: true,
		},
		{
			name: "invalid UUID",
			data: []byte(`{
				"eventId": "not-a-uuid",
				"matchId": "match-123",
				"eventType": "goal",
				"timestamp": "2024-01-01T12:00:00Z",
				"teamId": 1
			}`),
			wantErr: true,
		},
		{
			name: "invalid timestamp",
			data: []byte(`{
				"eventId": "` + eventID.String() + `",
				"matchId": "match-123",
				"eventType": "goal",
				"timestamp": "invalid-time",
				"teamId": 1
			}`),
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			event, err := EventFromKafkaMessage(tt.data)
			if tt.wantErr {
				if err == nil {
					t.Error("EventFromKafkaMessage() expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("EventFromKafkaMessage() unexpected error: %v", err)
			}
			if event == nil {
				t.Fatal("EventFromKafkaMessage() returned nil event")
			}
		})
	}
}

func TestEvent_RoundTrip(t *testing.T) {
	original := &Event{
		EventID:   uuid.New(),
		MatchID:   "match-round-trip",
		EventType: EventTypeFoul,
		Timestamp: time.Now().UTC().Truncate(time.Nanosecond),
		TeamID:    2,
		PlayerID:  "player-99",
		Metadata:  map[string]interface{}{"severity": "minor"},
	}

	// Serialize to Kafka message
	data, err := original.ToKafkaMessage()
	if err != nil {
		t.Fatalf("ToKafkaMessage() error: %v", err)
	}

	// Deserialize back
	restored, err := EventFromKafkaMessage(data)
	if err != nil {
		t.Fatalf("EventFromKafkaMessage() error: %v", err)
	}

	// Verify fields
	if restored.EventID != original.EventID {
		t.Errorf("EventID mismatch: got %v, want %v", restored.EventID, original.EventID)
	}
	if restored.MatchID != original.MatchID {
		t.Errorf("MatchID mismatch: got %q, want %q", restored.MatchID, original.MatchID)
	}
	if restored.EventType != original.EventType {
		t.Errorf("EventType mismatch: got %q, want %q", restored.EventType, original.EventType)
	}
	if restored.TeamID != original.TeamID {
		t.Errorf("TeamID mismatch: got %d, want %d", restored.TeamID, original.TeamID)
	}
	if restored.PlayerID != original.PlayerID {
		t.Errorf("PlayerID mismatch: got %q, want %q", restored.PlayerID, original.PlayerID)
	}
}

// TestEventRequest_ToEvent_AllEventTypes tests conversion for all valid event types
func TestEventRequest_ToEvent_AllEventTypes(t *testing.T) {
	validUUID := uuid.New().String()
	validTimestamp := time.Now().UTC().Format(time.RFC3339)

	tests := []struct {
		name          string
		eventType     string
		wantEventType EventType
	}{
		{"pass event", "pass", EventTypePass},
		{"shot event", "shot", EventTypeShot},
		{"goal event", "goal", EventTypeGoal},
		{"foul event", "foul", EventTypeFoul},
		{"yellow card event", "yellow_card", EventTypeYellowCard},
		{"red card event", "red_card", EventTypeRedCard},
		{"substitution event", "substitution", EventTypeSubstitution},
		{"offside event", "offside", EventTypeOffside},
		{"corner event", "corner", EventTypeCorner},
		{"free kick event", "free_kick", EventTypeFreeKick},
		{"interception event", "interception", EventTypeInterception},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := EventRequest{
				EventID:   validUUID,
				MatchID:   "match-123",
				EventType: tt.eventType,
				Timestamp: validTimestamp,
				TeamID:    1,
				PlayerID:  "player-1",
			}

			event, err := req.ToEvent()
			if err != nil {
				t.Fatalf("ToEvent() unexpected error for %s: %v", tt.eventType, err)
			}
			if event.EventType != tt.wantEventType {
				t.Errorf("ToEvent().EventType = %q, want %q", event.EventType, tt.wantEventType)
			}
		})
	}
}

// TestEventRequest_ToEvent_BoundaryTeamIDs tests edge cases for teamId validation
func TestEventRequest_ToEvent_BoundaryTeamIDs(t *testing.T) {
	validUUID := uuid.New().String()
	validTimestamp := time.Now().UTC().Format(time.RFC3339)

	tests := []struct {
		name    string
		teamID  int
		wantErr bool
	}{
		{"team 1 is valid", 1, false},
		{"team 2 is valid", 2, false},
		{"team 0 is invalid", 0, true},
		{"team 3 is invalid", 3, true},
		{"negative team is invalid", -1, true},
		{"very large team is invalid", 999, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := EventRequest{
				EventID:   validUUID,
				MatchID:   "match-123",
				EventType: "pass",
				Timestamp: validTimestamp,
				TeamID:    tt.teamID,
				PlayerID:  "player-1",
			}

			event, err := req.ToEvent()
			if tt.wantErr {
				if err == nil {
					t.Errorf("ToEvent() expected error for teamID %d, got nil", tt.teamID)
				}
				if event != nil {
					t.Errorf("ToEvent() expected nil event for invalid teamID")
				}
			} else {
				if err != nil {
					t.Errorf("ToEvent() unexpected error for teamID %d: %v", tt.teamID, err)
				}
				if event == nil {
					t.Errorf("ToEvent() returned nil event for valid teamID %d", tt.teamID)
				}
			}
		})
	}
}

// TestEventRequest_ToEvent_TimestampFormats tests various timestamp formats
func TestEventRequest_ToEvent_TimestampFormats(t *testing.T) {
	validUUID := uuid.New().String()

	tests := []struct {
		name      string
		timestamp string
		wantErr   bool
	}{
		{"RFC3339 format", "2024-06-15T10:30:00Z", false},
		{"RFC3339 with timezone offset", "2024-06-15T10:30:00+05:30", false},
		{"RFC3339 with negative offset", "2024-06-15T10:30:00-08:00", false},
		{"empty timestamp", "", true},
		{"date only", "2024-06-15", true},
		{"time only", "10:30:00", true},
		{"Unix timestamp", "1718444800", true},
		{"ISO 8601 without T", "2024-06-15 10:30:00Z", true},
		{"invalid month", "2024-13-15T10:30:00Z", true},
		{"invalid day", "2024-06-32T10:30:00Z", true},
		{"random string", "not-a-timestamp", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := EventRequest{
				EventID:   validUUID,
				MatchID:   "match-123",
				EventType: "pass",
				Timestamp: tt.timestamp,
				TeamID:    1,
				PlayerID:  "player-1",
			}

			event, err := req.ToEvent()
			if tt.wantErr {
				if err == nil {
					t.Errorf("ToEvent() expected error for timestamp %q, got nil", tt.timestamp)
				}
			} else {
				if err != nil {
					t.Errorf("ToEvent() unexpected error for timestamp %q: %v", tt.timestamp, err)
				}
				if event == nil {
					t.Errorf("ToEvent() returned nil event for valid timestamp")
				}
			}
		})
	}
}

// TestEventRequest_ToEvent_MetadataVariations tests various metadata scenarios
func TestEventRequest_ToEvent_MetadataVariations(t *testing.T) {
	validUUID := uuid.New().String()
	validTimestamp := time.Now().UTC().Format(time.RFC3339)

	tests := []struct {
		name     string
		metadata map[string]interface{}
	}{
		{"nil metadata", nil},
		{"empty metadata", map[string]interface{}{}},
		{"single string value", map[string]interface{}{"key": "value"}},
		{"single int value", map[string]interface{}{"count": 42}},
		{"single bool value", map[string]interface{}{"on_target": true}},
		{"single float value", map[string]interface{}{"distance": 15.5}},
		{"multiple values", map[string]interface{}{"x": 50, "y": 30, "zone": "penalty_area"}},
		{"nested object", map[string]interface{}{"position": map[string]interface{}{"x": 50, "y": 30}}},
		{"array value", map[string]interface{}{"tags": []string{"important", "replay"}}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := EventRequest{
				EventID:   validUUID,
				MatchID:   "match-123",
				EventType: "shot",
				Timestamp: validTimestamp,
				TeamID:    1,
				PlayerID:  "player-1",
				Metadata:  tt.metadata,
			}

			event, err := req.ToEvent()
			if err != nil {
				t.Fatalf("ToEvent() unexpected error: %v", err)
			}
			if event == nil {
				t.Fatal("ToEvent() returned nil event")
			}

			// Verify metadata is preserved
			if tt.metadata == nil {
				if event.Metadata != nil {
					t.Errorf("expected nil metadata, got %v", event.Metadata)
				}
			} else {
				if len(event.Metadata) != len(tt.metadata) {
					t.Errorf("metadata length mismatch: got %d, want %d", len(event.Metadata), len(tt.metadata))
				}
			}
		})
	}
}

// TestEventRequest_ToEvent_UUIDEdgeCases tests UUID validation edge cases
func TestEventRequest_ToEvent_UUIDEdgeCases(t *testing.T) {
	validTimestamp := time.Now().UTC().Format(time.RFC3339)

	tests := []struct {
		name    string
		eventID string
		wantErr bool
	}{
		{"valid UUID v4", "550e8400-e29b-41d4-a716-446655440000", false},
		{"valid UUID v1", "6ba7b810-9dad-11d1-80b4-00c04fd430c8", false},
		{"uppercase UUID", "550E8400-E29B-41D4-A716-446655440000", false},
		{"nil UUID", "00000000-0000-0000-0000-000000000000", false},
		{"empty string", "", true},
		{"missing hyphens", "550e8400e29b41d4a716446655440000", false}, // uuid.Parse accepts this
		{"too short", "550e8400-e29b-41d4", true},
		{"too long", "550e8400-e29b-41d4-a716-446655440000-extra", true},
		{"invalid characters", "550e8400-e29b-41d4-a716-44665544GGGG", true},
		{"spaces in UUID", "550e8400 e29b 41d4 a716 446655440000", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := EventRequest{
				EventID:   tt.eventID,
				MatchID:   "match-123",
				EventType: "pass",
				Timestamp: validTimestamp,
				TeamID:    1,
				PlayerID:  "player-1",
			}

			event, err := req.ToEvent()
			if tt.wantErr {
				if err == nil {
					t.Errorf("ToEvent() expected error for eventID %q, got nil", tt.eventID)
				}
			} else {
				if err != nil {
					t.Errorf("ToEvent() unexpected error for eventID %q: %v", tt.eventID, err)
				}
				if event == nil {
					t.Errorf("ToEvent() returned nil event for valid eventID")
				}
			}
		})
	}
}

// TestEventRequest_ToEvent_MatchIDEdgeCases tests matchId validation edge cases
func TestEventRequest_ToEvent_MatchIDEdgeCases(t *testing.T) {
	validUUID := uuid.New().String()
	validTimestamp := time.Now().UTC().Format(time.RFC3339)

	tests := []struct {
		name    string
		matchID string
		wantErr bool
	}{
		{"simple match ID", "match-123", false},
		{"UUID as match ID", uuid.New().String(), false},
		{"numeric match ID", "12345", false},
		{"match ID with underscores", "match_season_2024_game_1", false},
		{"match ID with special chars", "match-123@league", false},
		{"empty match ID", "", true},
		{"whitespace only", "   ", false}, // Note: not validated as whitespace
		{"very long match ID", "match-" + string(make([]byte, 1000)), false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := EventRequest{
				EventID:   validUUID,
				MatchID:   tt.matchID,
				EventType: "pass",
				Timestamp: validTimestamp,
				TeamID:    1,
				PlayerID:  "player-1",
			}

			event, err := req.ToEvent()
			if tt.wantErr {
				if err == nil {
					t.Errorf("ToEvent() expected error for matchID %q, got nil", tt.matchID)
				}
				if event != nil {
					t.Error("ToEvent() expected nil event for invalid matchID")
				}
			} else {
				if err != nil {
					t.Errorf("ToEvent() unexpected error for matchID %q: %v", tt.matchID, err)
				}
				if event == nil {
					t.Error("ToEvent() returned nil event for valid matchID")
				}
			}
		})
	}
}

// TestEventRequest_ToEvent_PlayerIDEdgeCases tests playerID handling (optional field)
func TestEventRequest_ToEvent_PlayerIDEdgeCases(t *testing.T) {
	validUUID := uuid.New().String()
	validTimestamp := time.Now().UTC().Format(time.RFC3339)

	tests := []struct {
		name     string
		playerID string
	}{
		{"empty player ID", ""},
		{"simple player ID", "player-1"},
		{"UUID as player ID", uuid.New().String()},
		{"numeric player ID", "12345"},
		{"player ID with special chars", "player@team#10"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := EventRequest{
				EventID:   validUUID,
				MatchID:   "match-123",
				EventType: "corner",
				Timestamp: validTimestamp,
				TeamID:    1,
				PlayerID:  tt.playerID,
			}

			event, err := req.ToEvent()
			if err != nil {
				t.Fatalf("ToEvent() unexpected error for playerID %q: %v", tt.playerID, err)
			}
			if event.PlayerID != tt.playerID {
				t.Errorf("PlayerID = %q, want %q", event.PlayerID, tt.playerID)
			}
		})
	}
}

// TestEvent_MetadataJSON_EdgeCases tests additional metadata JSON serialization cases
func TestEvent_MetadataJSON_EdgeCases(t *testing.T) {
	tests := []struct {
		name           string
		metadata       map[string]interface{}
		expectedOutput string
		checkValid     bool
	}{
		{
			name:           "nil metadata",
			metadata:       nil,
			expectedOutput: "{}",
			checkValid:     false,
		},
		{
			name:           "empty map",
			metadata:       map[string]interface{}{},
			expectedOutput: "{}",
			checkValid:     false,
		},
		{
			name:           "string value",
			metadata:       map[string]interface{}{"name": "test"},
			expectedOutput: `{"name":"test"}`,
			checkValid:     false,
		},
		{
			name:           "integer value",
			metadata:       map[string]interface{}{"count": 42},
			expectedOutput: `{"count":42}`,
			checkValid:     false,
		},
		{
			name:           "float value",
			metadata:       map[string]interface{}{"distance": 15.5},
			expectedOutput: `{"distance":15.5}`,
			checkValid:     false,
		},
		{
			name:           "boolean value",
			metadata:       map[string]interface{}{"active": true},
			expectedOutput: `{"active":true}`,
			checkValid:     false,
		},
		{
			name:           "null value",
			metadata:       map[string]interface{}{"nullable": nil},
			expectedOutput: `{"nullable":null}`,
			checkValid:     false,
		},
		{
			name:       "multiple keys - check valid JSON only",
			metadata:   map[string]interface{}{"a": 1, "b": 2, "c": 3},
			checkValid: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			event := &Event{
				EventID:  uuid.New(),
				Metadata: tt.metadata,
			}

			result := event.MetadataJSON()

			if tt.checkValid {
				var parsed map[string]interface{}
				if err := json.Unmarshal([]byte(result), &parsed); err != nil {
					t.Errorf("MetadataJSON() returned invalid JSON: %v", err)
				}
				if len(parsed) != len(tt.metadata) {
					t.Errorf("MetadataJSON() key count = %d, want %d", len(parsed), len(tt.metadata))
				}
			} else {
				if result != tt.expectedOutput {
					t.Errorf("MetadataJSON() = %q, want %q", result, tt.expectedOutput)
				}
			}
		})
	}
}

// TestEventFromKafkaMessage_EdgeCases tests edge cases for Kafka message deserialization
func TestEventFromKafkaMessage_EdgeCases(t *testing.T) {
	validUUID := uuid.New()

	tests := []struct {
		name    string
		data    []byte
		wantErr bool
	}{
		{
			name:    "empty data",
			data:    []byte{},
			wantErr: true,
		},
		{
			name:    "null JSON",
			data:    []byte("null"),
			wantErr: true,
		},
		{
			name:    "empty JSON object",
			data:    []byte("{}"),
			wantErr: true,
		},
		{
			name:    "array instead of object",
			data:    []byte("[]"),
			wantErr: true,
		},
		{
			name: "missing eventId",
			data: []byte(`{
				"matchId": "match-123",
				"eventType": "goal",
				"timestamp": "2024-01-01T12:00:00Z",
				"teamId": 1
			}`),
			wantErr: true,
		},
		{
			name: "valid minimal message",
			data: []byte(`{
				"eventId": "` + validUUID.String() + `",
				"matchId": "match-123",
				"eventType": "goal",
				"timestamp": "2024-01-01T12:00:00Z",
				"teamId": 1,
				"playerId": ""
			}`),
			wantErr: false,
		},
		{
			name: "extra unknown fields are ignored",
			data: []byte(`{
				"eventId": "` + validUUID.String() + `",
				"matchId": "match-123",
				"eventType": "goal",
				"timestamp": "2024-01-01T12:00:00Z",
				"teamId": 1,
				"playerId": "",
				"unknownField": "ignored"
			}`),
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			event, err := EventFromKafkaMessage(tt.data)
			if tt.wantErr {
				if err == nil {
					t.Error("EventFromKafkaMessage() expected error, got nil")
				}
			} else {
				if err != nil {
					t.Errorf("EventFromKafkaMessage() unexpected error: %v", err)
				}
				if event == nil {
					t.Error("EventFromKafkaMessage() returned nil event")
				}
			}
		})
	}
}

// TestEventType_Constants verifies all event type constants are correctly defined
func TestEventType_Constants(t *testing.T) {
	tests := []struct {
		eventType EventType
		expected  string
	}{
		{EventTypePass, "pass"},
		{EventTypeShot, "shot"},
		{EventTypeGoal, "goal"},
		{EventTypeFoul, "foul"},
		{EventTypeYellowCard, "yellow_card"},
		{EventTypeRedCard, "red_card"},
		{EventTypeSubstitution, "substitution"},
		{EventTypeOffside, "offside"},
		{EventTypeCorner, "corner"},
		{EventTypeFreeKick, "free_kick"},
		{EventTypeInterception, "interception"},
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			if string(tt.eventType) != tt.expected {
				t.Errorf("EventType constant = %q, want %q", tt.eventType, tt.expected)
			}
		})
	}
}

// TestValidEventTypes_Completeness ensures ValidEventTypes map matches constants
func TestValidEventTypes_Completeness(t *testing.T) {
	expectedCount := 11 // Total number of event types

	if len(ValidEventTypes) != expectedCount {
		t.Errorf("ValidEventTypes has %d entries, want %d", len(ValidEventTypes), expectedCount)
	}

	// Ensure all are set to true
	for eventType, isValid := range ValidEventTypes {
		if !isValid {
			t.Errorf("ValidEventTypes[%q] = false, should be true", eventType)
		}
	}
}

// TestEventRequest_ToEvent_ValidationOrder tests that validations happen in correct order
func TestEventRequest_ToEvent_ValidationOrder(t *testing.T) {
	// Test with multiple invalid fields - UUID should be validated first
	req := EventRequest{
		EventID:   "invalid-uuid",
		MatchID:   "",
		EventType: "invalid-type",
		Timestamp: "invalid-timestamp",
		TeamID:    0,
		PlayerID:  "player-1",
	}

	_, err := req.ToEvent()
	if err == nil {
		t.Fatal("expected validation error")
	}

	ve := AsValidationError(err)
	if ve == nil {
		t.Fatal("expected ValidationError type")
	}

	// UUID validation should come first
	if ve.Field != "eventId" {
		t.Errorf("expected first validation error on 'eventId', got %q", ve.Field)
	}
}
