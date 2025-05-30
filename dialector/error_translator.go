package dialector

import (
	"errors"
	"regexp"

	"gorm.io/gorm"
)

var (
	// InfluxDB error message patterns
	rxMeasurementNotFound     = regexp.MustCompile(`(?i)measurement "(.+)" not found`)
	rxDuplicateData           = regexp.MustCompile(`(?i)duplicate data`)
	rxFieldTypeMismatch       = regexp.MustCompile(`(?i)field type conflict`)
	rxSyntaxError             = regexp.MustCompile(`(?i)syntax error`)
	rxInvalidTimePrecision    = regexp.MustCompile(`(?i)invalid time precision`)
	rxInsufficientPermissions = regexp.MustCompile(`(?i)insufficient permissions`)
	rxDatabaseNotFound        = regexp.MustCompile(`(?i)database "(.+)" not found`)
	rxServerUnavailable       = regexp.MustCompile(`(?i)(connection refused|server unavailable)`)
)

// TranslateError translates InfluxDB specific errors to GORM errors
func (dialector Dialector) TranslateError(err error) error {
	if err == nil {
		return nil
	}

	// Check if it's already a GORM error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return err
	}

	// Get the error message
	msg := err.Error()

	// Translate common InfluxDB errors to GORM errors
	switch {
	case rxMeasurementNotFound.MatchString(msg) || rxDatabaseNotFound.MatchString(msg):
		return gorm.ErrRecordNotFound
	case rxDuplicateData.MatchString(msg):
		return errors.New("ERROR: duplicate data detected (23000)")
	case rxFieldTypeMismatch.MatchString(msg):
		return errors.New("ERROR: field type conflict (22001)")
	case rxInsufficientPermissions.MatchString(msg):
		return errors.New("ERROR: insufficient permissions (42000)")
	case rxSyntaxError.MatchString(msg):
		return errors.New("ERROR: syntax error (42000)")
	case rxServerUnavailable.MatchString(msg):
		return errors.New("ERROR: server unavailable (08S01)")
	default:
		// Return the original error for unhandled cases
		return err
	}
}
