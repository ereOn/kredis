package kredis

import "time"

var timeZero = time.Time{}

// An ErrorItem represents an error.
type ErrorItem struct {
	Error error
	Count int
}

// ErrorFeed represents an error feed.
type ErrorFeed struct {
	Threshold      time.Duration
	firstErrorTime time.Time
	timeFunc       func() time.Time
	errors         []ErrorItem
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
		f.errors = []ErrorItem{
			ErrorItem{
				Error: err,
				Count: 1,
			},
		}
	} else {
		new := true
		for i, item := range f.errors {
			if item.Error.Error() == err.Error() {
				new = false
				f.errors[i].Count++
			}
		}

		if new {
			f.errors = append(f.errors, ErrorItem{
				Error: err,
				Count: 1,
			})
		}
	}
}

// PopErrors pops the list of errors.
func (f *ErrorFeed) PopErrors() []ErrorItem {
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
