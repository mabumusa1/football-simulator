package domain

import (
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
