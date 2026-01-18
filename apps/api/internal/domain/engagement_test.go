package domain

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestEngagementEventRequest_ToEngagementEvent_ValidRequest(t *testing.T) {
	validUUID := uuid.New().String()
	validTimestamp := time.Now().UTC().Format(time.RFC3339)

	tests := []struct {
		name           string
		request        EngagementEventRequest
		wantMatchID    string
		wantUserID     string
		wantType       EngagementType
		wantDeviceType string
	}{
		{
			name: "valid reaction engagement",
			request: EngagementEventRequest{
				EventID:           validUUID,
				MatchID:           "match-123",
				UserID:            "user-456",
				SessionID:         "session-789",
				EngagementType:    "reaction",
				EngagementSubtype: "cheer",
				GameMinute:        45,
				DeviceType:        "mobile",
				Platform:          "ios",
				CountryCode:       "US",
				Timestamp:         validTimestamp,
			},
			wantMatchID:    "match-123",
			wantUserID:     "user-456",
			wantType:       EngagementTypeReaction,
			wantDeviceType: string(DeviceMobile),
		},
		{
			name: "valid comment engagement",
			request: EngagementEventRequest{
				EventID:           validUUID,
				MatchID:           "match-456",
				UserID:            "user-789",
				SessionID:         "session-012",
				EngagementType:    "comment",
				EngagementSubtype: "match_commentary",
				GameMinute:        30,
				DeviceType:        "desktop",
				Platform:          "web",
				CountryCode:       "UK",
				Content:           "Great match!",
				Timestamp:         validTimestamp,
			},
			wantMatchID:    "match-456",
			wantUserID:     "user-789",
			wantType:       EngagementTypeComment,
			wantDeviceType: string(DeviceDesktop),
		},
		{
			name: "valid session engagement",
			request: EngagementEventRequest{
				EventID:           validUUID,
				MatchID:           "match-789",
				UserID:            "user-012",
				SessionID:         "session-345",
				EngagementType:    "session",
				EngagementSubtype: "join",
				GameMinute:        0,
				DeviceType:        "tablet",
				Platform:          "android",
				CountryCode:       "DE",
				Timestamp:         validTimestamp,
			},
			wantMatchID:    "match-789",
			wantUserID:     "user-012",
			wantType:       EngagementTypeSession,
			wantDeviceType: string(DeviceTablet),
		},
		{
			name: "valid engagement with TV device",
			request: EngagementEventRequest{
				EventID:           validUUID,
				MatchID:           "match-abc",
				UserID:            "user-def",
				SessionID:         "session-ghi",
				EngagementType:    "video_action",
				EngagementSubtype: "pause",
				GameMinute:        60,
				DeviceType:        "tv",
				Platform:          "smart_tv",
				CountryCode:       "FR",
				Timestamp:         validTimestamp,
			},
			wantMatchID:    "match-abc",
			wantUserID:     "user-def",
			wantType:       EngagementTypeVideoAction,
			wantDeviceType: string(DeviceTV),
		},
		{
			name: "engagement with related game event",
			request: EngagementEventRequest{
				EventID:            validUUID,
				MatchID:            "match-xyz",
				UserID:             "user-xyz",
				SessionID:          "session-xyz",
				EngagementType:     "reaction",
				EngagementSubtype:  "emoji_goal",
				RelatedGameEventID: strPtr(uuid.New().String()),
				GameMinute:         75,
				DeviceType:         "mobile",
				Timestamp:          validTimestamp,
			},
			wantMatchID:    "match-xyz",
			wantUserID:     "user-xyz",
			wantType:       EngagementTypeReaction,
			wantDeviceType: string(DeviceMobile),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			event, err := tt.request.ToEngagementEvent()
			if err != nil {
				t.Fatalf("ToEngagementEvent() unexpected error: %v", err)
			}
			if event == nil {
				t.Fatal("ToEngagementEvent() returned nil event")
			}
			if event.MatchID != tt.wantMatchID {
				t.Errorf("ToEngagementEvent().MatchID = %q, want %q", event.MatchID, tt.wantMatchID)
			}
			if event.UserID != tt.wantUserID {
				t.Errorf("ToEngagementEvent().UserID = %q, want %q", event.UserID, tt.wantUserID)
			}
			if event.EngagementType != tt.wantType {
				t.Errorf("ToEngagementEvent().EngagementType = %q, want %q", event.EngagementType, tt.wantType)
			}
			if event.DeviceType != tt.wantDeviceType {
				t.Errorf("ToEngagementEvent().DeviceType = %q, want %q", event.DeviceType, tt.wantDeviceType)
			}
		})
	}
}

func TestEngagementEventRequest_ToEngagementEvent_InvalidUUID(t *testing.T) {
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
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := EngagementEventRequest{
				EventID:        tt.eventID,
				MatchID:        "match-123",
				UserID:         "user-456",
				SessionID:      "session-789",
				EngagementType: "reaction",
				GameMinute:     45,
				DeviceType:     "mobile",
				Timestamp:      time.Now().UTC().Format(time.RFC3339),
			}
			event, err := req.ToEngagementEvent()
			if err == nil {
				t.Error("ToEngagementEvent() expected error for invalid UUID, got nil")
			}
			if event != nil {
				t.Error("ToEngagementEvent() expected nil event for invalid UUID")
			}
			ve := AsValidationError(err)
			if ve == nil {
				t.Error("ToEngagementEvent() expected ValidationError")
			} else if ve.Field != "event_id" {
				t.Errorf("ValidationError.Field = %q, want %q", ve.Field, "event_id")
			}
		})
	}
}

func TestEngagementEventRequest_ToEngagementEvent_InvalidMatchID(t *testing.T) {
	req := EngagementEventRequest{
		EventID:        uuid.New().String(),
		MatchID:        "", // Empty matchID
		UserID:         "user-456",
		SessionID:      "session-789",
		EngagementType: "reaction",
		GameMinute:     45,
		DeviceType:     "mobile",
		Timestamp:      time.Now().UTC().Format(time.RFC3339),
	}

	event, err := req.ToEngagementEvent()
	if err == nil {
		t.Error("ToEngagementEvent() expected error for empty match_id, got nil")
	}
	if event != nil {
		t.Error("ToEngagementEvent() expected nil event for empty match_id")
	}
	ve := AsValidationError(err)
	if ve == nil {
		t.Error("ToEngagementEvent() expected ValidationError")
	} else if ve.Field != "match_id" {
		t.Errorf("ValidationError.Field = %q, want %q", ve.Field, "match_id")
	}
}

func TestEngagementEventRequest_ToEngagementEvent_InvalidUserID(t *testing.T) {
	req := EngagementEventRequest{
		EventID:        uuid.New().String(),
		MatchID:        "match-123",
		UserID:         "", // Empty userID
		SessionID:      "session-789",
		EngagementType: "reaction",
		GameMinute:     45,
		DeviceType:     "mobile",
		Timestamp:      time.Now().UTC().Format(time.RFC3339),
	}

	event, err := req.ToEngagementEvent()
	if err == nil {
		t.Error("ToEngagementEvent() expected error for empty user_id, got nil")
	}
	if event != nil {
		t.Error("ToEngagementEvent() expected nil event for empty user_id")
	}
	ve := AsValidationError(err)
	if ve == nil {
		t.Error("ToEngagementEvent() expected ValidationError")
	} else if ve.Field != "user_id" {
		t.Errorf("ValidationError.Field = %q, want %q", ve.Field, "user_id")
	}
}

func TestEngagementEventRequest_ToEngagementEvent_InvalidSessionID(t *testing.T) {
	req := EngagementEventRequest{
		EventID:        uuid.New().String(),
		MatchID:        "match-123",
		UserID:         "user-456",
		SessionID:      "", // Empty sessionID
		EngagementType: "reaction",
		GameMinute:     45,
		DeviceType:     "mobile",
		Timestamp:      time.Now().UTC().Format(time.RFC3339),
	}

	event, err := req.ToEngagementEvent()
	if err == nil {
		t.Error("ToEngagementEvent() expected error for empty session_id, got nil")
	}
	if event != nil {
		t.Error("ToEngagementEvent() expected nil event for empty session_id")
	}
	ve := AsValidationError(err)
	if ve == nil {
		t.Error("ToEngagementEvent() expected ValidationError")
	} else if ve.Field != "session_id" {
		t.Errorf("ValidationError.Field = %q, want %q", ve.Field, "session_id")
	}
}

func TestEngagementEventRequest_ToEngagementEvent_InvalidEngagementType(t *testing.T) {
	tests := []struct {
		name           string
		engagementType string
	}{
		{
			name:           "empty engagement type",
			engagementType: "",
		},
		{
			name:           "invalid engagement type",
			engagementType: "invalid_type",
		},
		{
			name:           "uppercase engagement type",
			engagementType: "REACTION",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := EngagementEventRequest{
				EventID:        uuid.New().String(),
				MatchID:        "match-123",
				UserID:         "user-456",
				SessionID:      "session-789",
				EngagementType: tt.engagementType,
				GameMinute:     45,
				DeviceType:     "mobile",
				Timestamp:      time.Now().UTC().Format(time.RFC3339),
			}
			event, err := req.ToEngagementEvent()
			if err == nil {
				t.Error("ToEngagementEvent() expected error for invalid engagement_type, got nil")
			}
			if event != nil {
				t.Error("ToEngagementEvent() expected nil event for invalid engagement_type")
			}
			ve := AsValidationError(err)
			if ve == nil {
				t.Error("ToEngagementEvent() expected ValidationError")
			} else if ve.Field != "engagement_type" {
				t.Errorf("ValidationError.Field = %q, want %q", ve.Field, "engagement_type")
			}
		})
	}
}

func TestEngagementEventRequest_ToEngagementEvent_InvalidRelatedGameEventID(t *testing.T) {
	invalidUUID := "not-a-valid-uuid"
	req := EngagementEventRequest{
		EventID:            uuid.New().String(),
		MatchID:            "match-123",
		UserID:             "user-456",
		SessionID:          "session-789",
		EngagementType:     "reaction",
		RelatedGameEventID: &invalidUUID,
		GameMinute:         45,
		DeviceType:         "mobile",
		Timestamp:          time.Now().UTC().Format(time.RFC3339),
	}

	event, err := req.ToEngagementEvent()
	if err == nil {
		t.Error("ToEngagementEvent() expected error for invalid related_game_event_id, got nil")
	}
	if event != nil {
		t.Error("ToEngagementEvent() expected nil event for invalid related_game_event_id")
	}
	ve := AsValidationError(err)
	if ve == nil {
		t.Error("ToEngagementEvent() expected ValidationError")
	} else if ve.Field != "related_game_event_id" {
		t.Errorf("ValidationError.Field = %q, want %q", ve.Field, "related_game_event_id")
	}
}

func TestEngagementEventRequest_ToEngagementEvent_InvalidGameMinute(t *testing.T) {
	tests := []struct {
		name       string
		gameMinute int
	}{
		{
			name:       "negative game minute",
			gameMinute: -1,
		},
		{
			name:       "game minute above 120",
			gameMinute: 121,
		},
		{
			name:       "very large game minute",
			gameMinute: 1000,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := EngagementEventRequest{
				EventID:        uuid.New().String(),
				MatchID:        "match-123",
				UserID:         "user-456",
				SessionID:      "session-789",
				EngagementType: "reaction",
				GameMinute:     tt.gameMinute,
				DeviceType:     "mobile",
				Timestamp:      time.Now().UTC().Format(time.RFC3339),
			}
			event, err := req.ToEngagementEvent()
			if err == nil {
				t.Errorf("ToEngagementEvent() expected error for game_minute %d, got nil", tt.gameMinute)
			}
			if event != nil {
				t.Error("ToEngagementEvent() expected nil event for invalid game_minute")
			}
			ve := AsValidationError(err)
			if ve == nil {
				t.Error("ToEngagementEvent() expected ValidationError")
			} else if ve.Field != "game_minute" {
				t.Errorf("ValidationError.Field = %q, want %q", ve.Field, "game_minute")
			}
		})
	}
}

func TestEngagementEventRequest_ToEngagementEvent_ValidGameMinuteBoundary(t *testing.T) {
	tests := []struct {
		name       string
		gameMinute int
	}{
		{
			name:       "game minute 0 (start)",
			gameMinute: 0,
		},
		{
			name:       "game minute 45 (half-time)",
			gameMinute: 45,
		},
		{
			name:       "game minute 90 (regular end)",
			gameMinute: 90,
		},
		{
			name:       "game minute 120 (extra time end)",
			gameMinute: 120,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := EngagementEventRequest{
				EventID:        uuid.New().String(),
				MatchID:        "match-123",
				UserID:         "user-456",
				SessionID:      "session-789",
				EngagementType: "reaction",
				GameMinute:     tt.gameMinute,
				DeviceType:     "mobile",
				Timestamp:      time.Now().UTC().Format(time.RFC3339),
			}
			event, err := req.ToEngagementEvent()
			if err != nil {
				t.Fatalf("ToEngagementEvent() unexpected error for game_minute %d: %v", tt.gameMinute, err)
			}
			if event.GameMinute != tt.gameMinute {
				t.Errorf("ToEngagementEvent().GameMinute = %d, want %d", event.GameMinute, tt.gameMinute)
			}
		})
	}
}

func TestEngagementEventRequest_ToEngagementEvent_UnknownDeviceType(t *testing.T) {
	req := EngagementEventRequest{
		EventID:        uuid.New().String(),
		MatchID:        "match-123",
		UserID:         "user-456",
		SessionID:      "session-789",
		EngagementType: "reaction",
		GameMinute:     45,
		DeviceType:     "smartwatch", // Unknown device type
		Timestamp:      time.Now().UTC().Format(time.RFC3339),
	}

	event, err := req.ToEngagementEvent()
	if err != nil {
		t.Fatalf("ToEngagementEvent() unexpected error: %v", err)
	}
	// Unknown device types should default to DeviceUnknown
	if event.DeviceType != string(DeviceUnknown) {
		t.Errorf("ToEngagementEvent().DeviceType = %q, want %q", event.DeviceType, string(DeviceUnknown))
	}
}

func TestEngagementEventRequest_ToEngagementEvent_TimestampParsing(t *testing.T) {
	tests := []struct {
		name      string
		timestamp string
		wantErr   bool
	}{
		{
			name:      "valid RFC3339 timestamp",
			timestamp: "2024-01-15T12:00:00Z",
			wantErr:   false,
		},
		{
			name:      "valid RFC3339Nano timestamp",
			timestamp: "2024-01-15T12:00:00.123456789Z",
			wantErr:   false,
		},
		{
			name:      "invalid timestamp uses current time",
			timestamp: "invalid-timestamp",
			wantErr:   false, // Falls back to current time
		},
		{
			name:      "empty timestamp uses current time",
			timestamp: "",
			wantErr:   false, // Falls back to current time
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := EngagementEventRequest{
				EventID:        uuid.New().String(),
				MatchID:        "match-123",
				UserID:         "user-456",
				SessionID:      "session-789",
				EngagementType: "reaction",
				GameMinute:     45,
				DeviceType:     "mobile",
				Timestamp:      tt.timestamp,
			}
			event, err := req.ToEngagementEvent()
			if tt.wantErr {
				if err == nil {
					t.Error("ToEngagementEvent() expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("ToEngagementEvent() unexpected error: %v", err)
			}
			if event.Timestamp.IsZero() {
				t.Error("ToEngagementEvent().Timestamp should not be zero")
			}
		})
	}
}

func TestValidEngagementTypes(t *testing.T) {
	expectedTypes := []EngagementType{
		EngagementTypeReaction,
		EngagementTypeComment,
		EngagementTypeVideoAction,
		EngagementTypeShare,
		EngagementTypePrediction,
		EngagementTypeClick,
		EngagementTypeSession,
	}

	for _, et := range expectedTypes {
		if !ValidEngagementTypes[et] {
			t.Errorf("EngagementType %q should be valid", et)
		}
	}

	if len(ValidEngagementTypes) != len(expectedTypes) {
		t.Errorf("ValidEngagementTypes has %d entries, want %d", len(ValidEngagementTypes), len(expectedTypes))
	}
}

func TestValidDeviceTypes(t *testing.T) {
	expectedTypes := []DeviceType{
		DeviceMobile,
		DeviceDesktop,
		DeviceTablet,
		DeviceTV,
		DeviceUnknown,
	}

	for _, dt := range expectedTypes {
		if !ValidDeviceTypes[dt] {
			t.Errorf("DeviceType %q should be valid", dt)
		}
	}

	if len(ValidDeviceTypes) != len(expectedTypes) {
		t.Errorf("ValidDeviceTypes has %d entries, want %d", len(ValidDeviceTypes), len(expectedTypes))
	}
}

func TestEngagementSubtypes(t *testing.T) {
	// Test reaction subtypes
	reactionSubtypes := EngagementSubtypes[EngagementTypeReaction]
	expectedReactionSubtypes := []string{"cheer", "boo", "emoji_goal", "emoji_fire", "emoji_clap", "emoji_cry", "emoji_angry", "emoji_heart", "emoji_laugh", "emoji_wow"}
	for _, subtype := range expectedReactionSubtypes {
		if !reactionSubtypes[subtype] {
			t.Errorf("Reaction subtype %q should be valid", subtype)
		}
	}

	// Test session subtypes
	sessionSubtypes := EngagementSubtypes[EngagementTypeSession]
	expectedSessionSubtypes := []string{"join", "leave", "heartbeat", "reconnect"}
	for _, subtype := range expectedSessionSubtypes {
		if !sessionSubtypes[subtype] {
			t.Errorf("Session subtype %q should be valid", subtype)
		}
	}
}

func TestEngagementEvent_MetadataJSON(t *testing.T) {
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
			metadata: map[string]interface{}{"duration": 5.5},
			want:     `{"duration":5.5}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			event := &EngagementEvent{
				EventID:  uuid.New(),
				Metadata: tt.metadata,
			}
			got := event.MetadataJSON()

			if tt.want != "" {
				if got != tt.want {
					t.Errorf("MetadataJSON() = %q, want %q", got, tt.want)
				}
			}
		})
	}
}

func TestEngagementEvent_ToKafkaMessage(t *testing.T) {
	eventID := uuid.New()
	relatedEventID := uuid.New()
	timestamp := time.Now().UTC()

	event := &EngagementEvent{
		EventID:            eventID,
		MatchID:            "match-123",
		UserID:             "user-456",
		SessionID:          "session-789",
		EngagementType:     EngagementTypeReaction,
		EngagementSubtype:  "cheer",
		RelatedGameEventID: &relatedEventID,
		GameMinute:         45,
		DeviceType:         string(DeviceMobile),
		Platform:           "ios",
		CountryCode:        "US",
		Content:            "Great goal!",
		Metadata:           map[string]interface{}{"intensity": 10},
		Timestamp:          timestamp,
	}

	data, err := event.ToKafkaMessage()
	if err != nil {
		t.Fatalf("ToKafkaMessage() unexpected error: %v", err)
	}

	var msg EngagementKafkaMessage
	if err := json.Unmarshal(data, &msg); err != nil {
		t.Fatalf("ToKafkaMessage() returned invalid JSON: %v", err)
	}

	if msg.EventID != eventID.String() {
		t.Errorf("KafkaMessage.EventID = %q, want %q", msg.EventID, eventID.String())
	}
	if msg.MatchID != "match-123" {
		t.Errorf("KafkaMessage.MatchID = %q, want %q", msg.MatchID, "match-123")
	}
	if msg.UserID != "user-456" {
		t.Errorf("KafkaMessage.UserID = %q, want %q", msg.UserID, "user-456")
	}
	if msg.EngagementType != "reaction" {
		t.Errorf("KafkaMessage.EngagementType = %q, want %q", msg.EngagementType, "reaction")
	}
	if msg.DeviceType != "mobile" {
		t.Errorf("KafkaMessage.DeviceType = %q, want %q", msg.DeviceType, "mobile")
	}
	if msg.RelatedGameEventID == nil || *msg.RelatedGameEventID != relatedEventID.String() {
		t.Error("KafkaMessage.RelatedGameEventID mismatch")
	}
}

func TestEngagementEvent_ToKafkaMessage_NilRelatedEventID(t *testing.T) {
	event := &EngagementEvent{
		EventID:            uuid.New(),
		MatchID:            "match-123",
		UserID:             "user-456",
		SessionID:          "session-789",
		EngagementType:     EngagementTypeSession,
		EngagementSubtype:  "join",
		RelatedGameEventID: nil,
		GameMinute:         0,
		DeviceType:         string(DeviceMobile),
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
		t.Errorf("KafkaMessage.RelatedGameEventID should be nil, got %v", *msg.RelatedGameEventID)
	}
}

// Helper function for creating string pointers
func strPtr(s string) *string {
	return &s
}

// TestEngagementEventRequest_ToEngagementEvent_AllEngagementTypes tests all valid engagement types
func TestEngagementEventRequest_ToEngagementEvent_AllEngagementTypes(t *testing.T) {
	validUUID := uuid.New().String()
	validTimestamp := time.Now().UTC().Format(time.RFC3339)

	tests := []struct {
		name           string
		engagementType string
		wantType       EngagementType
	}{
		{"reaction type", "reaction", EngagementTypeReaction},
		{"comment type", "comment", EngagementTypeComment},
		{"video_action type", "video_action", EngagementTypeVideoAction},
		{"share type", "share", EngagementTypeShare},
		{"prediction type", "prediction", EngagementTypePrediction},
		{"click type", "click", EngagementTypeClick},
		{"session type", "session", EngagementTypeSession},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := EngagementEventRequest{
				EventID:        validUUID,
				MatchID:        "match-123",
				UserID:         "user-456",
				SessionID:      "session-789",
				EngagementType: tt.engagementType,
				GameMinute:     45,
				DeviceType:     "mobile",
				Timestamp:      validTimestamp,
			}

			event, err := req.ToEngagementEvent()
			if err != nil {
				t.Fatalf("ToEngagementEvent() unexpected error for %s: %v", tt.engagementType, err)
			}
			if event.EngagementType != tt.wantType {
				t.Errorf("EngagementType = %q, want %q", event.EngagementType, tt.wantType)
			}
		})
	}
}

// TestEngagementEventRequest_ToEngagementEvent_AllDeviceTypes tests all valid device types
func TestEngagementEventRequest_ToEngagementEvent_AllDeviceTypes(t *testing.T) {
	validUUID := uuid.New().String()
	validTimestamp := time.Now().UTC().Format(time.RFC3339)

	tests := []struct {
		name           string
		deviceType     string
		wantDeviceType string
	}{
		{"mobile device", "mobile", string(DeviceMobile)},
		{"desktop device", "desktop", string(DeviceDesktop)},
		{"tablet device", "tablet", string(DeviceTablet)},
		{"tv device", "tv", string(DeviceTV)},
		{"unknown device", "unknown", string(DeviceUnknown)},
		{"invalid device defaults to unknown", "invalid_device", string(DeviceUnknown)},
		{"empty device defaults to unknown", "", string(DeviceUnknown)},
		{"uppercase device defaults to unknown", "MOBILE", string(DeviceUnknown)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := EngagementEventRequest{
				EventID:        validUUID,
				MatchID:        "match-123",
				UserID:         "user-456",
				SessionID:      "session-789",
				EngagementType: "reaction",
				GameMinute:     45,
				DeviceType:     tt.deviceType,
				Timestamp:      validTimestamp,
			}

			event, err := req.ToEngagementEvent()
			if err != nil {
				t.Fatalf("ToEngagementEvent() unexpected error: %v", err)
			}
			if event.DeviceType != tt.wantDeviceType {
				t.Errorf("DeviceType = %q, want %q", event.DeviceType, tt.wantDeviceType)
			}
		})
	}
}

// TestEngagementEventRequest_ToEngagementEvent_UUIDEdgeCases tests UUID validation edge cases
func TestEngagementEventRequest_ToEngagementEvent_UUIDEdgeCases(t *testing.T) {
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
		{"missing hyphens", "550e8400e29b41d4a716446655440000", false},
		{"too short", "550e8400-e29b-41d4", true},
		{"too long", "550e8400-e29b-41d4-a716-446655440000-extra", true},
		{"invalid characters", "550e8400-e29b-41d4-a716-44665544GGGG", true},
		{"spaces in UUID", "550e8400 e29b 41d4 a716 446655440000", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := EngagementEventRequest{
				EventID:        tt.eventID,
				MatchID:        "match-123",
				UserID:         "user-456",
				SessionID:      "session-789",
				EngagementType: "reaction",
				GameMinute:     45,
				DeviceType:     "mobile",
				Timestamp:      validTimestamp,
			}

			event, err := req.ToEngagementEvent()
			if tt.wantErr {
				if err == nil {
					t.Errorf("ToEngagementEvent() expected error for event_id %q, got nil", tt.eventID)
				}
			} else {
				if err != nil {
					t.Errorf("ToEngagementEvent() unexpected error for event_id %q: %v", tt.eventID, err)
				}
				if event == nil {
					t.Errorf("ToEngagementEvent() returned nil event for valid event_id")
				}
			}
		})
	}
}

// TestEngagementEventRequest_ToEngagementEvent_RelatedGameEventIDEdgeCases tests related event ID edge cases
func TestEngagementEventRequest_ToEngagementEvent_RelatedGameEventIDEdgeCases(t *testing.T) {
	validUUID := uuid.New().String()
	validTimestamp := time.Now().UTC().Format(time.RFC3339)
	relatedUUID := uuid.New().String()
	invalidUUID := "not-a-uuid"
	emptyString := ""

	tests := []struct {
		name               string
		relatedGameEventID *string
		wantNil            bool
		wantErr            bool
	}{
		{"nil related event ID", nil, true, false},
		{"empty related event ID", &emptyString, true, false},
		{"valid related event ID", &relatedUUID, false, false},
		{"invalid related event ID", &invalidUUID, false, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := EngagementEventRequest{
				EventID:            validUUID,
				MatchID:            "match-123",
				UserID:             "user-456",
				SessionID:          "session-789",
				EngagementType:     "reaction",
				RelatedGameEventID: tt.relatedGameEventID,
				GameMinute:         45,
				DeviceType:         "mobile",
				Timestamp:          validTimestamp,
			}

			event, err := req.ToEngagementEvent()
			if tt.wantErr {
				if err == nil {
					t.Error("ToEngagementEvent() expected error, got nil")
				}
				return
			}

			if err != nil {
				t.Fatalf("ToEngagementEvent() unexpected error: %v", err)
			}

			if tt.wantNil {
				if event.RelatedGameEventID != nil {
					t.Errorf("RelatedGameEventID = %v, want nil", event.RelatedGameEventID)
				}
			} else {
				if event.RelatedGameEventID == nil {
					t.Error("RelatedGameEventID = nil, want non-nil")
				}
			}
		})
	}
}

// TestEngagementEventRequest_ToEngagementEvent_ContentField tests content handling
func TestEngagementEventRequest_ToEngagementEvent_ContentField(t *testing.T) {
	validUUID := uuid.New().String()
	validTimestamp := time.Now().UTC().Format(time.RFC3339)

	tests := []struct {
		name    string
		content string
	}{
		{"empty content", ""},
		{"simple text", "Hello world!"},
		{"text with special chars", "Great goal! @player #match"},
		{"unicode content", "Gooool! \u26BD\uFE0F"},
		{"long content", "This is a very long comment that goes on and on and describes the amazing play we just witnessed on the pitch."},
		{"content with newlines", "Line 1\nLine 2\nLine 3"},
		{"content with quotes", `He said "What a goal!" and I agreed.`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := EngagementEventRequest{
				EventID:           validUUID,
				MatchID:           "match-123",
				UserID:            "user-456",
				SessionID:         "session-789",
				EngagementType:    "comment",
				EngagementSubtype: "match_commentary",
				GameMinute:        45,
				DeviceType:        "mobile",
				Content:           tt.content,
				Timestamp:         validTimestamp,
			}

			event, err := req.ToEngagementEvent()
			if err != nil {
				t.Fatalf("ToEngagementEvent() unexpected error: %v", err)
			}
			if event.Content != tt.content {
				t.Errorf("Content = %q, want %q", event.Content, tt.content)
			}
		})
	}
}

// TestEngagementEventRequest_ToEngagementEvent_MetadataVariations tests metadata handling
func TestEngagementEventRequest_ToEngagementEvent_MetadataVariations(t *testing.T) {
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
		{"single bool value", map[string]interface{}{"active": true}},
		{"single float value", map[string]interface{}{"distance": 15.5}},
		{"multiple values", map[string]interface{}{"x": 50, "y": 30, "zone": "penalty_area"}},
		{"nested object", map[string]interface{}{"position": map[string]interface{}{"x": 50, "y": 30}}},
		{"array value", map[string]interface{}{"tags": []string{"important", "replay"}}},
		{"null value", map[string]interface{}{"optional": nil}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := EngagementEventRequest{
				EventID:        validUUID,
				MatchID:        "match-123",
				UserID:         "user-456",
				SessionID:      "session-789",
				EngagementType: "reaction",
				GameMinute:     45,
				DeviceType:     "mobile",
				Metadata:       tt.metadata,
				Timestamp:      validTimestamp,
			}

			event, err := req.ToEngagementEvent()
			if err != nil {
				t.Fatalf("ToEngagementEvent() unexpected error: %v", err)
			}

			if tt.metadata == nil {
				if event.Metadata != nil {
					t.Errorf("Metadata = %v, want nil", event.Metadata)
				}
			} else {
				if len(event.Metadata) != len(tt.metadata) {
					t.Errorf("Metadata length = %d, want %d", len(event.Metadata), len(tt.metadata))
				}
			}
		})
	}
}

// TestEngagementEventRequest_ToEngagementEvent_SubtypesByType tests subtypes for each engagement type
func TestEngagementEventRequest_ToEngagementEvent_SubtypesByType(t *testing.T) {
	validUUID := uuid.New().String()
	validTimestamp := time.Now().UTC().Format(time.RFC3339)

	// Test valid subtypes for each type
	tests := []struct {
		name              string
		engagementType    string
		engagementSubtype string
	}{
		// Reaction subtypes
		{"reaction - cheer", "reaction", "cheer"},
		{"reaction - boo", "reaction", "boo"},
		{"reaction - emoji_goal", "reaction", "emoji_goal"},
		{"reaction - emoji_fire", "reaction", "emoji_fire"},
		{"reaction - emoji_clap", "reaction", "emoji_clap"},
		{"reaction - emoji_cry", "reaction", "emoji_cry"},
		{"reaction - emoji_angry", "reaction", "emoji_angry"},
		{"reaction - emoji_heart", "reaction", "emoji_heart"},
		{"reaction - emoji_laugh", "reaction", "emoji_laugh"},
		{"reaction - emoji_wow", "reaction", "emoji_wow"},

		// Comment subtypes
		{"comment - match_commentary", "comment", "match_commentary"},
		{"comment - player_discussion", "comment", "player_discussion"},
		{"comment - team_support", "comment", "team_support"},
		{"comment - trash_talk", "comment", "trash_talk"},
		{"comment - question", "comment", "question"},

		// Video action subtypes
		{"video_action - pause", "video_action", "pause"},
		{"video_action - play", "video_action", "play"},
		{"video_action - rewind", "video_action", "rewind"},
		{"video_action - replay", "video_action", "replay"},
		{"video_action - camera_switch", "video_action", "camera_switch"},
		{"video_action - quality_change", "video_action", "quality_change"},
		{"video_action - fullscreen", "video_action", "fullscreen"},

		// Share subtypes
		{"share - twitter", "share", "twitter"},
		{"share - facebook", "share", "facebook"},
		{"share - whatsapp", "share", "whatsapp"},
		{"share - instagram", "share", "instagram"},
		{"share - in_app", "share", "in_app"},
		{"share - copy_link", "share", "copy_link"},

		// Prediction subtypes
		{"prediction - score_prediction", "prediction", "score_prediction"},
		{"prediction - next_goal", "prediction", "next_goal"},
		{"prediction - player_rating", "prediction", "player_rating"},
		{"prediction - poll_vote", "prediction", "poll_vote"},
		{"prediction - man_of_match", "prediction", "man_of_match"},

		// Click subtypes
		{"click - stats_view", "click", "stats_view"},
		{"click - player_profile", "click", "player_profile"},
		{"click - team_info", "click", "team_info"},
		{"click - lineup", "click", "lineup"},
		{"click - ad_click", "click", "ad_click"},
		{"click - merchandise", "click", "merchandise"},
		{"click - ticket", "click", "ticket"},

		// Session subtypes
		{"session - join", "session", "join"},
		{"session - leave", "session", "leave"},
		{"session - heartbeat", "session", "heartbeat"},
		{"session - reconnect", "session", "reconnect"},

		// Empty subtype should be allowed
		{"empty subtype", "reaction", ""},

		// Unknown subtype should be allowed (not rejected)
		{"unknown subtype", "reaction", "unknown_custom_subtype"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := EngagementEventRequest{
				EventID:           validUUID,
				MatchID:           "match-123",
				UserID:            "user-456",
				SessionID:         "session-789",
				EngagementType:    tt.engagementType,
				EngagementSubtype: tt.engagementSubtype,
				GameMinute:        45,
				DeviceType:        "mobile",
				Timestamp:         validTimestamp,
			}

			event, err := req.ToEngagementEvent()
			if err != nil {
				t.Fatalf("ToEngagementEvent() unexpected error: %v", err)
			}
			if event.EngagementSubtype != tt.engagementSubtype {
				t.Errorf("EngagementSubtype = %q, want %q", event.EngagementSubtype, tt.engagementSubtype)
			}
		})
	}
}

// TestEngagementEventRequest_ToEngagementEvent_TimestampFormats tests various timestamp formats
func TestEngagementEventRequest_ToEngagementEvent_TimestampFormats(t *testing.T) {
	validUUID := uuid.New().String()
	now := time.Now().UTC()

	tests := []struct {
		name           string
		timestamp      string
		expectFallback bool
	}{
		{"RFC3339 format", now.Format(time.RFC3339), false},
		{"RFC3339Nano format", now.Format(time.RFC3339Nano), false},
		{"RFC3339 with positive timezone offset", "2024-06-15T10:30:00+05:30", false},
		{"RFC3339 with negative timezone offset", "2024-06-15T10:30:00-08:00", false},
		{"invalid timestamp uses fallback", "invalid-timestamp", true},
		{"empty timestamp uses fallback", "", true},
		{"date only uses fallback", "2024-06-15", true},
		{"time only uses fallback", "10:30:00", true},
		{"Unix timestamp uses fallback", "1718444800", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			beforeTest := time.Now().UTC()

			req := EngagementEventRequest{
				EventID:        validUUID,
				MatchID:        "match-123",
				UserID:         "user-456",
				SessionID:      "session-789",
				EngagementType: "reaction",
				GameMinute:     45,
				DeviceType:     "mobile",
				Timestamp:      tt.timestamp,
			}

			event, err := req.ToEngagementEvent()
			if err != nil {
				t.Fatalf("ToEngagementEvent() unexpected error: %v", err)
			}

			afterTest := time.Now().UTC()

			if tt.expectFallback {
				// Timestamp should be set to approximately current time
				if event.Timestamp.Before(beforeTest.Add(-time.Second)) || event.Timestamp.After(afterTest.Add(time.Second)) {
					t.Errorf("Timestamp = %v, expected between %v and %v", event.Timestamp, beforeTest, afterTest)
				}
			}
		})
	}
}

// TestEngagementEventRequest_ToEngagementEvent_OptionalFields tests optional field handling
func TestEngagementEventRequest_ToEngagementEvent_OptionalFields(t *testing.T) {
	validUUID := uuid.New().String()
	validTimestamp := time.Now().UTC().Format(time.RFC3339)

	req := EngagementEventRequest{
		EventID:           validUUID,
		MatchID:           "match-123",
		UserID:            "user-456",
		SessionID:         "session-789",
		EngagementType:    "reaction",
		EngagementSubtype: "", // optional
		GameMinute:        0,  // edge case: minute 0
		DeviceType:        "mobile",
		Platform:          "",  // optional
		CountryCode:       "",  // optional
		Content:           "",  // optional
		Metadata:          nil, // optional
		Timestamp:         validTimestamp,
	}

	event, err := req.ToEngagementEvent()
	if err != nil {
		t.Fatalf("ToEngagementEvent() unexpected error: %v", err)
	}

	if event.Platform != "" {
		t.Errorf("Platform = %q, want empty string", event.Platform)
	}
	if event.CountryCode != "" {
		t.Errorf("CountryCode = %q, want empty string", event.CountryCode)
	}
	if event.Content != "" {
		t.Errorf("Content = %q, want empty string", event.Content)
	}
	if event.Metadata != nil {
		t.Errorf("Metadata = %v, want nil", event.Metadata)
	}
	if event.EngagementSubtype != "" {
		t.Errorf("EngagementSubtype = %q, want empty string", event.EngagementSubtype)
	}
}

// TestEngagementEventRequest_ToEngagementEvent_ValidationOrder tests validation order
func TestEngagementEventRequest_ToEngagementEvent_ValidationOrder(t *testing.T) {
	validTimestamp := time.Now().UTC().Format(time.RFC3339)

	// Test with multiple invalid fields - event_id should be validated first
	req := EngagementEventRequest{
		EventID:        "invalid-uuid",
		MatchID:        "",
		UserID:         "",
		SessionID:      "",
		EngagementType: "invalid",
		GameMinute:     -1,
		DeviceType:     "mobile",
		Timestamp:      validTimestamp,
	}

	_, err := req.ToEngagementEvent()
	if err == nil {
		t.Fatal("expected validation error")
	}

	ve := AsValidationError(err)
	if ve == nil {
		t.Fatal("expected ValidationError type")
	}

	// event_id validation should come first
	if ve.Field != "event_id" {
		t.Errorf("expected first validation error on 'event_id', got %q", ve.Field)
	}
}

// TestEngagementType_Constants tests engagement type constants
func TestEngagementType_Constants(t *testing.T) {
	tests := []struct {
		engagementType EngagementType
		expected       string
	}{
		{EngagementTypeReaction, "reaction"},
		{EngagementTypeComment, "comment"},
		{EngagementTypeVideoAction, "video_action"},
		{EngagementTypeShare, "share"},
		{EngagementTypePrediction, "prediction"},
		{EngagementTypeClick, "click"},
		{EngagementTypeSession, "session"},
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			if string(tt.engagementType) != tt.expected {
				t.Errorf("EngagementType constant = %q, want %q", tt.engagementType, tt.expected)
			}
		})
	}
}

// TestDeviceType_Constants tests device type constants
func TestDeviceType_Constants(t *testing.T) {
	tests := []struct {
		deviceType DeviceType
		expected   string
	}{
		{DeviceMobile, "mobile"},
		{DeviceDesktop, "desktop"},
		{DeviceTablet, "tablet"},
		{DeviceTV, "tv"},
		{DeviceUnknown, "unknown"},
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			if string(tt.deviceType) != tt.expected {
				t.Errorf("DeviceType constant = %q, want %q", tt.deviceType, tt.expected)
			}
		})
	}
}

// TestEngagementSubtypes_Completeness tests that all engagement types have subtypes defined
func TestEngagementSubtypes_Completeness(t *testing.T) {
	for engType := range ValidEngagementTypes {
		subtypes := EngagementSubtypes[engType]
		if subtypes == nil {
			t.Errorf("EngagementSubtypes[%q] is nil, expected a map of subtypes", engType)
			continue
		}
		if len(subtypes) == 0 {
			t.Errorf("EngagementSubtypes[%q] is empty, expected at least one subtype", engType)
		}
		// Ensure all subtypes are true
		for subtype, valid := range subtypes {
			if !valid {
				t.Errorf("EngagementSubtypes[%q][%q] = false, expected true", engType, subtype)
			}
		}
	}
}

// TestEngagementEvent_MetadataJSON_EdgeCases tests additional metadata JSON serialization cases
func TestEngagementEvent_MetadataJSON_EdgeCases(t *testing.T) {
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
			metadata:       map[string]interface{}{"score": 9.5},
			expectedOutput: `{"score":9.5}`,
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
			event := &EngagementEvent{
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

// TestEngagementBatchRequest tests batch request handling
func TestEngagementBatchRequest(t *testing.T) {
	validUUID1 := uuid.New().String()
	validUUID2 := uuid.New().String()
	validTimestamp := time.Now().UTC().Format(time.RFC3339)

	batch := EngagementBatchRequest{
		Events: []EngagementEventRequest{
			{
				EventID:        validUUID1,
				MatchID:        "match-123",
				UserID:         "user-456",
				SessionID:      "session-789",
				EngagementType: "reaction",
				GameMinute:     45,
				DeviceType:     "mobile",
				Timestamp:      validTimestamp,
			},
			{
				EventID:        validUUID2,
				MatchID:        "match-123",
				UserID:         "user-789",
				SessionID:      "session-abc",
				EngagementType: "comment",
				GameMinute:     46,
				DeviceType:     "desktop",
				Timestamp:      validTimestamp,
			},
		},
	}

	if len(batch.Events) != 2 {
		t.Errorf("batch.Events length = %d, want 2", len(batch.Events))
	}

	// Validate each event in batch
	for i, eventReq := range batch.Events {
		event, err := eventReq.ToEngagementEvent()
		if err != nil {
			t.Errorf("Event %d validation failed: %v", i, err)
		}
		if event == nil {
			t.Errorf("Event %d is nil", i)
		}
	}
}

// TestEngagementBatchRequest_EmptyBatch tests empty batch handling
func TestEngagementBatchRequest_EmptyBatch(t *testing.T) {
	batch := EngagementBatchRequest{
		Events: []EngagementEventRequest{},
	}

	if len(batch.Events) != 0 {
		t.Errorf("batch.Events length = %d, want 0", len(batch.Events))
	}
}

// TestEngagementBatchRequest_NilEvents tests nil events handling
func TestEngagementBatchRequest_NilEvents(t *testing.T) {
	batch := EngagementBatchRequest{
		Events: nil,
	}

	if batch.Events != nil {
		t.Errorf("batch.Events = %v, want nil", batch.Events)
	}
}

// TestEngagementMetrics_Structure tests EngagementMetrics struct
func TestEngagementMetrics_Structure(t *testing.T) {
	metrics := EngagementMetrics{
		MatchID:          "match-123",
		TotalEngagements: 1000,
		UniqueUsers:      250,
		EngagementsByType: map[string]int64{
			"reaction": 500,
			"comment":  300,
			"share":    200,
		},
		EngagementsBySubtype: map[string]map[string]int64{
			"reaction": {"cheer": 300, "boo": 200},
		},
		DeviceBreakdown: map[string]int64{
			"mobile":  600,
			"desktop": 300,
			"tablet":  100,
		},
		PlatformBreakdown: map[string]int64{
			"ios":     400,
			"android": 200,
			"web":     400,
		},
		CountryBreakdown: map[string]int64{
			"US": 400,
			"UK": 300,
			"DE": 300,
		},
		PeakEngagement: &PeakEngagementMinute{
			GameMinute:      75,
			EngagementCount: 150,
			UniqueUsers:     80,
		},
		EngagementTimeline: []EngagementTimelinePoint{
			{GameMinute: 0, Engagements: 50, UniqueUsers: 30},
			{GameMinute: 45, Engagements: 100, UniqueUsers: 60},
			{GameMinute: 90, Engagements: 150, UniqueUsers: 80},
		},
	}

	if metrics.MatchID != "match-123" {
		t.Errorf("MatchID = %q, want %q", metrics.MatchID, "match-123")
	}
	if metrics.TotalEngagements != 1000 {
		t.Errorf("TotalEngagements = %d, want %d", metrics.TotalEngagements, 1000)
	}
	if metrics.UniqueUsers != 250 {
		t.Errorf("UniqueUsers = %d, want %d", metrics.UniqueUsers, 250)
	}
	if metrics.PeakEngagement == nil {
		t.Error("PeakEngagement = nil, want non-nil")
	}
	if len(metrics.EngagementTimeline) != 3 {
		t.Errorf("EngagementTimeline length = %d, want 3", len(metrics.EngagementTimeline))
	}
}

// TestPeakEngagementMinute_Structure tests PeakEngagementMinute struct
func TestPeakEngagementMinute_Structure(t *testing.T) {
	peak := PeakEngagementMinute{
		GameMinute:      75,
		EngagementCount: 150,
		UniqueUsers:     80,
	}

	if peak.GameMinute != 75 {
		t.Errorf("GameMinute = %d, want %d", peak.GameMinute, 75)
	}
	if peak.EngagementCount != 150 {
		t.Errorf("EngagementCount = %d, want %d", peak.EngagementCount, 150)
	}
	if peak.UniqueUsers != 80 {
		t.Errorf("UniqueUsers = %d, want %d", peak.UniqueUsers, 80)
	}
}

// TestEngagementTimelinePoint_Structure tests EngagementTimelinePoint struct
func TestEngagementTimelinePoint_Structure(t *testing.T) {
	point := EngagementTimelinePoint{
		GameMinute:  45,
		Engagements: 100,
		UniqueUsers: 60,
	}

	if point.GameMinute != 45 {
		t.Errorf("GameMinute = %d, want %d", point.GameMinute, 45)
	}
	if point.Engagements != 100 {
		t.Errorf("Engagements = %d, want %d", point.Engagements, 100)
	}
	if point.UniqueUsers != 60 {
		t.Errorf("UniqueUsers = %d, want %d", point.UniqueUsers, 60)
	}
}
