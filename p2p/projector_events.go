package p2p

import "strings"

func projectedEventDedupeKey(eventType, eventID, subject string) string {
	eventType = strings.TrimSpace(eventType)
	eventID = strings.TrimSpace(eventID)
	subject = strings.TrimSpace(subject)
	if eventType == "" || eventID == "" {
		return ""
	}
	if subject == "" {
		return eventType + ":" + eventID
	}
	return eventType + ":" + eventID + ":" + subject
}
