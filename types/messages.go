package types

import (
	"time"
)

type SyslogMessage struct {
    ID         int64     `json:"id"`
    Timestamp  time.Time `json:"timestamp"`
    Hostname   string    `json:"hostname"`
    Tag        string    `json:"tag"`
    Content    string    `json:"content"`
    Severity   int       `json:"severity"`
    Facility   int       `json:"facility"`
    Priority   int       `json:"priority"`
    RemoteIP   string    `json:"remote_ip"`
    Protocol   string    `json:"protocol"`
    RawMessage string    `json:"raw_message"`
    CreatedAt  time.Time `json:"created_at"`
}

func (m *SyslogMessage) SeverityString() string {
	severities := []string{
		"emergency", "alert", "critical", "error",
		"warning", "notice", "info", "debug",
	}
	if m.Severity < len(severities) {
		return severities[m.Severity]
	}
	return "unknown"
}

func (m *SyslogMessage) FacilityString() string {
	facilities := []string{
		"kern", "user", "mail", "daemon",
		"auth", "syslog", "lpr", "news",
		"uucp", "cron", "authpriv", "ftp",
		"ntp", "logaudit", "logalert", "clock",
		"local0", "local1", "local2", "local3",
		"local4", "local5", "local6", "local7",
	}
	if m.Facility < len(facilities) {
		return facilities[m.Facility]
	}
	return "unknown"
}