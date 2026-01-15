package domain

import (
	"testing"
	"time"
)

func TestNewMatchMetrics(t *testing.T) {
	matchID := "match-123"
	metrics := NewMatchMetrics(matchID)

	if metrics == nil {
		t.Fatal("NewMatchMetrics() returned nil")
	}

	if metrics.MatchID != matchID {
		t.Errorf("NewMatchMetrics().MatchID = %q, want %q", metrics.MatchID, matchID)
	}

	if metrics.TotalEvents != 0 {
		t.Errorf("NewMatchMetrics().TotalEvents = %d, want 0", metrics.TotalEvents)
	}

	if metrics.EventsByType == nil {
		t.Error("NewMatchMetrics().EventsByType should be initialized, got nil")
	}

	if len(metrics.EventsByType) != 0 {
		t.Errorf("NewMatchMetrics().EventsByType should be empty, got %d entries", len(metrics.EventsByType))
	}
}

func TestMatchMetrics_Fields(t *testing.T) {
	metrics := &MatchMetrics{
		MatchID:      "match-456",
		TotalEvents:  100,
		EventsByType: map[string]int64{"goal": 3, "pass": 50, "shot": 10},
		Goals:        3,
		YellowCards:  2,
		RedCards:     1,
	}

	if metrics.MatchID != "match-456" {
		t.Errorf("MatchMetrics.MatchID = %q, want %q", metrics.MatchID, "match-456")
	}

	if metrics.TotalEvents != 100 {
		t.Errorf("MatchMetrics.TotalEvents = %d, want 100", metrics.TotalEvents)
	}

	if metrics.Goals != 3 {
		t.Errorf("MatchMetrics.Goals = %d, want 3", metrics.Goals)
	}

	if metrics.YellowCards != 2 {
		t.Errorf("MatchMetrics.YellowCards = %d, want 2", metrics.YellowCards)
	}

	if metrics.RedCards != 1 {
		t.Errorf("MatchMetrics.RedCards = %d, want 1", metrics.RedCards)
	}

	if len(metrics.EventsByType) != 3 {
		t.Errorf("MatchMetrics.EventsByType should have 3 entries, got %d", len(metrics.EventsByType))
	}
}

func TestMatchMetrics_WithTimeFields(t *testing.T) {
	now := time.Now().UTC()
	later := now.Add(time.Hour)

	metrics := &MatchMetrics{
		MatchID:      "match-789",
		TotalEvents:  50,
		EventsByType: make(map[string]int64),
		FirstEventAt: &now,
		LastEventAt:  &later,
	}

	if metrics.FirstEventAt == nil {
		t.Fatal("MatchMetrics.FirstEventAt should not be nil")
	}

	if !metrics.FirstEventAt.Equal(now) {
		t.Errorf("MatchMetrics.FirstEventAt = %v, want %v", *metrics.FirstEventAt, now)
	}

	if metrics.LastEventAt == nil {
		t.Fatal("MatchMetrics.LastEventAt should not be nil")
	}

	if !metrics.LastEventAt.Equal(later) {
		t.Errorf("MatchMetrics.LastEventAt = %v, want %v", *metrics.LastEventAt, later)
	}
}

func TestMatchMetrics_WithPeakMinute(t *testing.T) {
	peakTime := time.Now().UTC()
	peak := &PeakEngagement{
		Minute:     peakTime,
		EventCount: 25,
	}

	metrics := &MatchMetrics{
		MatchID:      "match-peak",
		TotalEvents:  100,
		EventsByType: make(map[string]int64),
		PeakMinute:   peak,
	}

	if metrics.PeakMinute == nil {
		t.Fatal("MatchMetrics.PeakMinute should not be nil")
	}

	if metrics.PeakMinute.EventCount != 25 {
		t.Errorf("MatchMetrics.PeakMinute.EventCount = %d, want 25", metrics.PeakMinute.EventCount)
	}
}

func TestMatchMetrics_WithResponseTimePercentiles(t *testing.T) {
	percentiles := &ResponseTimePercentiles{
		P50: 10.5,
		P95: 25.3,
		P99: 50.1,
	}

	metrics := &MatchMetrics{
		MatchID:                 "match-percentiles",
		TotalEvents:             200,
		EventsByType:            make(map[string]int64),
		ResponseTimePercentiles: percentiles,
	}

	if metrics.ResponseTimePercentiles == nil {
		t.Fatal("MatchMetrics.ResponseTimePercentiles should not be nil")
	}

	if metrics.ResponseTimePercentiles.P50 != 10.5 {
		t.Errorf("ResponseTimePercentiles.P50 = %f, want 10.5", metrics.ResponseTimePercentiles.P50)
	}

	if metrics.ResponseTimePercentiles.P95 != 25.3 {
		t.Errorf("ResponseTimePercentiles.P95 = %f, want 25.3", metrics.ResponseTimePercentiles.P95)
	}

	if metrics.ResponseTimePercentiles.P99 != 50.1 {
		t.Errorf("ResponseTimePercentiles.P99 = %f, want 50.1", metrics.ResponseTimePercentiles.P99)
	}
}

func TestPeakEngagement_Fields(t *testing.T) {
	peakTime := time.Date(2024, 1, 15, 12, 30, 0, 0, time.UTC)
	peak := PeakEngagement{
		Minute:     peakTime,
		EventCount: 42,
	}

	if !peak.Minute.Equal(peakTime) {
		t.Errorf("PeakEngagement.Minute = %v, want %v", peak.Minute, peakTime)
	}

	if peak.EventCount != 42 {
		t.Errorf("PeakEngagement.EventCount = %d, want 42", peak.EventCount)
	}
}

func TestEventsPerMinute_Fields(t *testing.T) {
	minuteTime := time.Date(2024, 1, 15, 12, 45, 0, 0, time.UTC)
	epm := EventsPerMinute{
		Minute:     minuteTime,
		EventType:  "goal",
		EventCount: 5,
	}

	if !epm.Minute.Equal(minuteTime) {
		t.Errorf("EventsPerMinute.Minute = %v, want %v", epm.Minute, minuteTime)
	}

	if epm.EventType != "goal" {
		t.Errorf("EventsPerMinute.EventType = %q, want %q", epm.EventType, "goal")
	}

	if epm.EventCount != 5 {
		t.Errorf("EventsPerMinute.EventCount = %d, want 5", epm.EventCount)
	}
}

func TestResponseTimePercentiles_Fields(t *testing.T) {
	percentiles := ResponseTimePercentiles{
		P50: 5.0,
		P95: 15.0,
		P99: 30.0,
	}

	if percentiles.P50 != 5.0 {
		t.Errorf("ResponseTimePercentiles.P50 = %f, want 5.0", percentiles.P50)
	}

	if percentiles.P95 != 15.0 {
		t.Errorf("ResponseTimePercentiles.P95 = %f, want 15.0", percentiles.P95)
	}

	if percentiles.P99 != 30.0 {
		t.Errorf("ResponseTimePercentiles.P99 = %f, want 30.0", percentiles.P99)
	}
}

func TestEngagementMetrics_Fields(t *testing.T) {
	metrics := EngagementMetrics{
		MatchID:          "match-eng-123",
		TotalEngagements: 1000,
		UniqueUsers:      250,
		EngagementsByType: map[string]int64{
			"reaction": 500,
			"comment":  200,
			"share":    100,
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
			"US": 300,
			"UK": 200,
			"DE": 150,
		},
	}

	if metrics.MatchID != "match-eng-123" {
		t.Errorf("EngagementMetrics.MatchID = %q, want %q", metrics.MatchID, "match-eng-123")
	}

	if metrics.TotalEngagements != 1000 {
		t.Errorf("EngagementMetrics.TotalEngagements = %d, want 1000", metrics.TotalEngagements)
	}

	if metrics.UniqueUsers != 250 {
		t.Errorf("EngagementMetrics.UniqueUsers = %d, want 250", metrics.UniqueUsers)
	}

	if metrics.EngagementsByType["reaction"] != 500 {
		t.Errorf("EngagementsByType[reaction] = %d, want 500", metrics.EngagementsByType["reaction"])
	}

	if metrics.DeviceBreakdown["mobile"] != 600 {
		t.Errorf("DeviceBreakdown[mobile] = %d, want 600", metrics.DeviceBreakdown["mobile"])
	}
}

func TestEngagementMetrics_WithPeakEngagement(t *testing.T) {
	peak := &PeakEngagementMinute{
		GameMinute:      75,
		EngagementCount: 150,
		UniqueUsers:     80,
	}

	metrics := EngagementMetrics{
		MatchID:          "match-peak-eng",
		TotalEngagements: 500,
		PeakEngagement:   peak,
	}

	if metrics.PeakEngagement == nil {
		t.Fatal("EngagementMetrics.PeakEngagement should not be nil")
	}

	if metrics.PeakEngagement.GameMinute != 75 {
		t.Errorf("PeakEngagement.GameMinute = %d, want 75", metrics.PeakEngagement.GameMinute)
	}

	if metrics.PeakEngagement.EngagementCount != 150 {
		t.Errorf("PeakEngagement.EngagementCount = %d, want 150", metrics.PeakEngagement.EngagementCount)
	}

	if metrics.PeakEngagement.UniqueUsers != 80 {
		t.Errorf("PeakEngagement.UniqueUsers = %d, want 80", metrics.PeakEngagement.UniqueUsers)
	}
}

func TestEngagementMetrics_WithTimeline(t *testing.T) {
	timeline := []EngagementTimelinePoint{
		{GameMinute: 0, Engagements: 50, UniqueUsers: 30},
		{GameMinute: 15, Engagements: 75, UniqueUsers: 45},
		{GameMinute: 30, Engagements: 100, UniqueUsers: 60},
		{GameMinute: 45, Engagements: 120, UniqueUsers: 70},
	}

	metrics := EngagementMetrics{
		MatchID:            "match-timeline",
		TotalEngagements:   345,
		EngagementTimeline: timeline,
	}

	if len(metrics.EngagementTimeline) != 4 {
		t.Fatalf("EngagementTimeline should have 4 entries, got %d", len(metrics.EngagementTimeline))
	}

	if metrics.EngagementTimeline[0].GameMinute != 0 {
		t.Errorf("EngagementTimeline[0].GameMinute = %d, want 0", metrics.EngagementTimeline[0].GameMinute)
	}

	if metrics.EngagementTimeline[3].Engagements != 120 {
		t.Errorf("EngagementTimeline[3].Engagements = %d, want 120", metrics.EngagementTimeline[3].Engagements)
	}
}

func TestPeakEngagementMinute_Fields(t *testing.T) {
	peak := PeakEngagementMinute{
		GameMinute:      90,
		EngagementCount: 200,
		UniqueUsers:     100,
	}

	if peak.GameMinute != 90 {
		t.Errorf("PeakEngagementMinute.GameMinute = %d, want 90", peak.GameMinute)
	}

	if peak.EngagementCount != 200 {
		t.Errorf("PeakEngagementMinute.EngagementCount = %d, want 200", peak.EngagementCount)
	}

	if peak.UniqueUsers != 100 {
		t.Errorf("PeakEngagementMinute.UniqueUsers = %d, want 100", peak.UniqueUsers)
	}
}

func TestEngagementTimelinePoint_Fields(t *testing.T) {
	point := EngagementTimelinePoint{
		GameMinute:  60,
		Engagements: 85,
		UniqueUsers: 50,
	}

	if point.GameMinute != 60 {
		t.Errorf("EngagementTimelinePoint.GameMinute = %d, want 60", point.GameMinute)
	}

	if point.Engagements != 85 {
		t.Errorf("EngagementTimelinePoint.Engagements = %d, want 85", point.Engagements)
	}

	if point.UniqueUsers != 50 {
		t.Errorf("EngagementTimelinePoint.UniqueUsers = %d, want 50", point.UniqueUsers)
	}
}
