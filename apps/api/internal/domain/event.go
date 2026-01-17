package domain

import (
	sharedDomain "github.com/mabumusa1/football-simulator/pkg/domain"
)

// Re-export types from shared domain package
type EventType = sharedDomain.EventType
type Event = sharedDomain.Event
type EventRequest = sharedDomain.EventRequest
type KafkaMessage = sharedDomain.KafkaMessage

// Re-export constants from shared domain package
const (
	EventTypePass         = sharedDomain.EventTypePass
	EventTypeShot         = sharedDomain.EventTypeShot
	EventTypeShotOnTarget = sharedDomain.EventTypeShotOnTarget
	EventTypeGoal         = sharedDomain.EventTypeGoal
	EventTypeOwnGoal      = sharedDomain.EventTypeOwnGoal
	EventTypeFoul         = sharedDomain.EventTypeFoul
	EventTypeYellowCard   = sharedDomain.EventTypeYellowCard
	EventTypeRedCard      = sharedDomain.EventTypeRedCard
	EventTypeSubstitution = sharedDomain.EventTypeSubstitution
	EventTypeOffside      = sharedDomain.EventTypeOffside
	EventTypeCorner       = sharedDomain.EventTypeCorner
	EventTypeFreeKick     = sharedDomain.EventTypeFreeKick
	EventTypePenalty      = sharedDomain.EventTypePenalty
	EventTypePenaltySaved = sharedDomain.EventTypePenaltySaved
	EventTypeInterception = sharedDomain.EventTypeInterception
	EventTypeTackle       = sharedDomain.EventTypeTackle
	EventTypeSave         = sharedDomain.EventTypeSave
	EventTypeInjury       = sharedDomain.EventTypeInjury
	EventTypeVARReview    = sharedDomain.EventTypeVARReview
	EventTypeKickoff      = sharedDomain.EventTypeKickoff
	EventTypeHalfTime     = sharedDomain.EventTypeHalfTime
	EventTypeSecondHalf   = sharedDomain.EventTypeSecondHalf
	EventTypeFullTime     = sharedDomain.EventTypeFullTime
)

// Re-export variables from shared domain package
var ValidEventTypes = sharedDomain.ValidEventTypes

// Re-export functions from shared domain package
var EventFromKafkaMessage = sharedDomain.EventFromKafkaMessage
