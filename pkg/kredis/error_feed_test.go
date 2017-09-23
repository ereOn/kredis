package kredis

import (
	"errors"
	"testing"
	"time"
)

func TestErrorFeedInit(t *testing.T) {
	feed := ErrorFeed{}
	feed.init()

	if feed.timeFunc == nil {
		t.Error("expected a timeFunc to be set")
	}

	// For coverage we call it.
	feed.timeFunc()
}

func TestErrorFeed(t *testing.T) {
	now := time.Now().UTC()

	feed := ErrorFeed{
		Threshold: time.Second,
		timeFunc:  func() time.Time { return now },
	}
	errA := errors.New("a")
	errB := errors.New("b")

	feed.Add(errA)
	feed.Add(errB)
	feed.Add(errA)

	items := feed.PopErrors()

	if len(items) != 0 {
		t.Errorf("expected no error items but got: %v", items)
	}

	now = now.Add(time.Second + 2)
	items = feed.PopErrors()

	if len(items) != 2 {
		t.Errorf("expected 2 error items but got: %v", items)
	}

	items = feed.PopErrors()

	if len(items) != 0 {
		t.Errorf("expected no error items after a successful pop but got: %v", items)
	}

}
