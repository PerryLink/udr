package processor

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/gin-gonic/gin"

	"github.com/free5gc/openapi/models"
	udr_context "github.com/free5gc/udr/internal/context"
)

// subsIdFromLocation returns the last path segment of a Location header.
func subsIdFromLocation(location string) string {
	if location == "" {
		return ""
	}
	return location[strings.LastIndex(location, "/")+1:]
}

// resetNotificationSubscriptionContext gives the process-wide context the two maps and the
// subscription-id counters that udr_context.Init() installs at start-up, and nothing else, so the
// test does not need a parsed configuration.
func resetNotificationSubscriptionContext() {
	udr := udr_context.GetSelf()
	udr.SubscriptionDataSubscriptions = make(map[string]*udr_context.SubscriptionDataSubscriptionRecord)
	udr.PolicyDataSubscriptions = make(map[string]*udr_context.PolicyDataSubscriptionRecord)
	udr.Reset() // also puts both subscription-id generators back to 1
}

// postSubscriptionData runs the real handler and returns the subsId it handed out, or "" if it
// did not answer with one. It takes no *testing.T: it is called from goroutines.
func postSubscriptionData(p *Processor) string {
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	p.PostSubscriptionDataSubscriptionsProcedure(c, models.Udr_DR_SubscriptionDataSubscriptions{})
	return subsIdFromLocation(c.Writer.Header().Get("Location"))
}

func postPolicyData(p *Processor) string {
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	p.PolicyDataSubsToNotifyPostProcedure(c, models.Udr_DR_PolicyDataSubscription{})
	return subsIdFromLocation(c.Writer.Header().Get("Location"))
}

// TestConcurrentPostAllocatesDistinctSubsIds checks the invariant the two maps and their
// subscription-id counters have to keep under concurrency: every created subscription gets its
// own id, and every record is kept.
func TestConcurrentPostAllocatesDistinctSubsIds(t *testing.T) {
	gin.SetMode(gin.TestMode)
	resetNotificationSubscriptionContext()
	p := &Processor{}

	const n = 64

	for _, tc := range []struct {
		name string
		post func(*Processor) string
		size func() int
	}{
		{
			name: "subscription data",
			post: postSubscriptionData,
			size: func() int { return len(udr_context.GetSelf().SubscriptionDataSubscriptions) },
		},
		{
			name: "policy data",
			post: postPolicyData,
			size: func() int { return len(udr_context.GetSelf().PolicyDataSubscriptions) },
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			resetNotificationSubscriptionContext()

			ids := make([]string, n)
			var wg sync.WaitGroup
			for i := 0; i < n; i++ {
				wg.Add(1)
				go func(i int) {
					defer wg.Done()
					ids[i] = tc.post(p)
				}(i)
			}
			wg.Wait()

			seen := make(map[string]bool, n)
			for i, id := range ids {
				if id == "" {
					t.Fatalf("request %d got no subsId", i)
				}
				if seen[id] {
					t.Errorf("subsId %q was handed out more than once", id)
				}
				seen[id] = true
			}
			if got := tc.size(); got != n {
				t.Errorf("%d subscriptions stored, want %d", got, n)
			}
		})
	}
}

// TestConcurrentDeleteOfOneSubsId checks that a subscription removed concurrently is removed once:
// exactly one request may answer 204, the rest have to answer 404, and none may panic.
func TestConcurrentDeleteOfOneSubsId(t *testing.T) {
	gin.SetMode(gin.TestMode)
	resetNotificationSubscriptionContext()
	p := &Processor{}

	subsId := postSubscriptionData(p)
	if subsId == "" {
		t.Fatal("could not create a subscription to delete")
	}

	const n = 32
	codes := make([]int, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			c, _ := gin.CreateTestContext(httptest.NewRecorder())
			p.RemovesubscriptionDataSubscriptionsProcedure(c, subsId)
			codes[i] = c.Writer.Status()
		}(i)
	}
	wg.Wait()

	var noContent, notFound int
	for i, code := range codes {
		switch code {
		case http.StatusNoContent:
			noContent++
		case http.StatusNotFound:
			notFound++
		default:
			t.Errorf("request %d answered %d, want 204 or 404", i, code)
		}
	}
	if noContent != 1 {
		t.Errorf("%d requests answered 204, want exactly 1", noContent)
	}
	if notFound != n-1 {
		t.Errorf("%d requests answered 404, want %d", notFound, n-1)
	}
	if got := len(udr_context.GetSelf().SubscriptionDataSubscriptions); got != 0 {
		t.Errorf("%d subscriptions left, want 0", got)
	}
}

// TestConcurrentPutAndReadOfOnePolicyDataSubscription checks that updating a stored record while
// another request reads it is serialised.
func TestConcurrentPutAndReadOfOnePolicyDataSubscription(t *testing.T) {
	gin.SetMode(gin.TestMode)
	resetNotificationSubscriptionContext()
	p := &Processor{}

	subsId := postPolicyData(p)
	if subsId == "" {
		t.Fatal("could not create a subscription to update")
	}

	const n = 32
	codes := make([]int, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			c, _ := gin.CreateTestContext(httptest.NewRecorder())
			if i%2 == 0 {
				p.PolicyDataSubsToNotifySubsIdPutProcedure(c, subsId, models.Udr_DR_PolicyDataSubscription{})
			} else {
				p.PolicyDataSubsToNotifySubsIdDeleteProcedure(c, subsId)
			}
			codes[i] = c.Writer.Status()
		}(i)
	}
	wg.Wait()

	for i, code := range codes {
		if code != http.StatusOK && code != http.StatusNoContent && code != http.StatusNotFound {
			t.Errorf("request %d answered %d", i, code)
		}
	}
}
