package syslog

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"syslog-server/types"
)

// RFC 3164 pattern (BSD syslog)
var bsdPattern = regexp.MustCompile(`^<(\d+)>(\w{3}\s+\d{1,2}\s+\d{2}:\d{2}:\d{2})\s+(\S+)\s+(\S+)?:?\s*(.*)$`)

// RFC 5424 pattern (structured data)
// simplePattern e compilat o singură dată (înainte: la fiecare mesaj nepotrivit).
var simplePattern = regexp.MustCompile(`^<(\d+)>(\S+)\s+(\S+)?:?\s*(.*)$`)

var rfc5424Pattern = regexp.MustCompile(`^<(\d+)>(\d+)\s+(\S+)\s+(\S+)\s+(\S+)\s+(\S+)\s+(\S*)\s+(.*)$`)

func ParseMessage(message string, remoteIP string) (*types.SyslogMessage, error) {

	message = strings.TrimSpace(message)
	if message == "" {
		return nil, fmt.Errorf("empty message")
	}

	// Try RFC 5424 first
	if msg := parseRFC5424(message, remoteIP); msg != nil {
		return msg, nil
	}

	// Try RFC 3164
	if msg := parseRFC3164(message, remoteIP); msg != nil {
		return msg, nil
	}

	// Try simple format (fără timestamp)
	if msg := parseSimpleFormat(message, remoteIP); msg != nil {
		return msg, nil
	}

	// If no standard format, create a basic message
	return &types.SyslogMessage{
		Timestamp:  time.Now(),
		Hostname:   "unknown",
		Content:    message,
		RemoteIP:   remoteIP,
		RawMessage: message,
		Severity:   6,  // info
		Facility:   1,  // user
		Priority:   14, // user.info
	}, nil
}

func parseRFC3164(message string, remoteIP string) *types.SyslogMessage {
	matches := bsdPattern.FindStringSubmatch(message)
	if matches == nil {
		return nil
	}

	priority, _ := strconv.Atoi(matches[1])
	facility := priority / 8
	severity := priority % 8

	// FIX: Parsăm timestamp cu timezone awareness
	timestamp := parseBSDTimestamp(matches[2])

	hostname := matches[3]
	tag := matches[4]
	content := matches[5]

	if tag == ":" || tag == "" {
		parts := strings.SplitN(content, ":", 2)
		if len(parts) > 1 {
			tag = strings.TrimSpace(parts[0])
			content = strings.TrimSpace(parts[1])

			if tag == "" {
				tag = "unknown"
			}
		} else {
			tag = ""
		}
	} else {
		tag = strings.TrimSuffix(tag, ":")
	}

	return &types.SyslogMessage{
		Timestamp:  timestamp,
		Hostname:   hostname,
		Tag:        tag,
		Content:    content,
		Severity:   severity,
		Facility:   facility,
		Priority:   priority,
		RemoteIP:   remoteIP,
		RawMessage: message,
	}
}

func parseRFC5424(message string, remoteIP string) *types.SyslogMessage {
	matches := rfc5424Pattern.FindStringSubmatch(message)
	if matches == nil {
		return nil
	}

	priority, _ := strconv.Atoi(matches[1])
	facility := priority / 8
	severity := priority % 8

	timestamp, err := time.Parse(time.RFC3339, matches[2])
	if err != nil {
		timestamp = time.Now()
	}

	return &types.SyslogMessage{
		Timestamp:  timestamp,
		Hostname:   matches[3],
		Tag:        matches[5],
		Content:    matches[8],
		Severity:   severity,
		Facility:   facility,
		Priority:   priority,
		RemoteIP:   remoteIP,
		RawMessage: message,
	}
}

// FIX: parseBSDTimestamp cu timezone awareness
func parseBSDTimestamp(timestamp string) time.Time {
	now := time.Now()
	currentYear := now.Year()

	// BSD format: "Dec 25 10:30:45"
	fullTimestamp := fmt.Sprintf("%d %s", currentYear, timestamp)

	layouts := []string{
		"2006 Jan 2 15:04:05",
		"2006 Jan 02 15:04:05",
	}

	var parsedTime time.Time
	var err error

	for _, layout := range layouts {
		parsedTime, err = time.ParseInLocation(layout, fullTimestamp, time.Local)
		if err == nil {
			break
		}
	}

	if err != nil {
		return now
	}

	// FIX: Verifică dacă timestamp-ul e în viitor (la schimbarea anului)
	// Dacă avem un mesaj din decembrie dar acum e ianuarie, anul trebuie decrementat
	if parsedTime.After(now.Add(24 * time.Hour)) {
		parsedTime = parsedTime.AddDate(-1, 0, 0)
	}

	return parsedTime
}

func parseSimpleFormat(message string, remoteIP string) *types.SyslogMessage {

	matches := simplePattern.FindStringSubmatch(message)
	if matches == nil {
		return nil
	}

	priority, _ := strconv.Atoi(matches[1])
	facility := priority / 8
	severity := priority % 8

	hostname := matches[2]
	tag := matches[3]
	content := matches[4]

	if tag == ":" || tag == "" {
		parts := strings.SplitN(content, ":", 2)
		if len(parts) > 1 {
			tag = strings.TrimSpace(parts[0])
			content = strings.TrimSpace(parts[1])
			if tag == "" {
				tag = "unknown"
			}
		} else {
			tag = ""
		}
	} else {
		tag = strings.TrimSuffix(tag, ":")
	}

	return &types.SyslogMessage{
		Timestamp:  time.Now(),
		Hostname:   hostname,
		Tag:        tag,
		Content:    content,
		Severity:   severity,
		Facility:   facility,
		Priority:   priority,
		RemoteIP:   remoteIP,
		RawMessage: message,
	}
}
