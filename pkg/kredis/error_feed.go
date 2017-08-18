package kredis

import "time"

var timeZero = time.Time{}

// ErrorFeed represents an error feed.
type ErrorFeed struct {
	Threshold      time.Duration
	firstErrorTime time.Time
	timeFunc       func() time.Time
	errors         []error
}

func (f *ErrorFeed) init() {
	if f.timeFunc == nil {
		f.timeFunc = func() time.Time { return time.Now().UTC() }
	}
}

// Add an error to the feed.
func (f *ErrorFeed) Add(err error) {
	f.init()

	if f.firstErrorTime == timeZero {
		f.firstErrorTime = f.timeFunc()
		f.errors = []error{err}
	} else {
		f.errors = append(f.errors, err)
	}
}

// PopErrors pops the list of errors.
func (f *ErrorFeed) PopErrors() []error {
	f.init()

	now := f.timeFunc()

	if f.firstErrorTime != timeZero && now.After(f.firstErrorTime.Add(f.Threshold)) {
		defer f.Reset()

		return f.errors
	}

	return nil
}

// Reset the list of errors.
func (f *ErrorFeed) Reset() {
	f.firstErrorTime = timeZero
	f.errors = nil
}
