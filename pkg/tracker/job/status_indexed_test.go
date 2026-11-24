package job

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/werf/kubedog/pkg/tracker/indicators"
	"github.com/werf/kubedog/pkg/tracker/pod"
)

func indexedMode() *batchv1.CompletionMode {
	m := batchv1.IndexedCompletion
	return &m
}

func nonIndexedMode() *batchv1.CompletionMode {
	m := batchv1.NonIndexedCompletion
	return &m
}

func strPtr(s string) *string { return &s }

func condition(cType batchv1.JobConditionType, status corev1.ConditionStatus, reason string) batchv1.JobCondition {
	return batchv1.JobCondition{
		Type:               cType,
		Status:             status,
		Reason:             reason,
		LastTransitionTime: metav1.Now(),
	}
}

func newIndexedJob(completedIndexes string, failedIndexes *string, conditions ...batchv1.JobCondition) *batchv1.Job {
	completions := int32(5)

	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Name: "idx", Namespace: "default"},
		Spec: batchv1.JobSpec{
			Completions:    &completions,
			CompletionMode: indexedMode(),
		},
		Status: batchv1.JobStatus{
			CompletedIndexes: completedIndexes,
			FailedIndexes:    failedIndexes,
			Conditions:       conditions,
		},
	}
}

func terminatedPod(name string, succeeded bool) pod.PodStatus {
	p := pod.PodStatus{
		Name:            name,
		StatusIndicator: &indicators.StringEqualConditionIndicator{},
		IsSucceeded:     succeeded,
		IsFailed:        !succeeded,
	}
	return p
}

// statusWithTerminatedPods runs NewJobStatus with every tracked pod already
// terminated, so controller conditions are always evaluated.
func statusWithTerminatedPods(t *testing.T, object *batchv1.Job, podNames ...string) JobStatus {
	t.Helper()

	pods := map[string]pod.PodStatus{}
	for _, n := range podNames {
		pods[n] = terminatedPod(n, true)
	}

	return NewJobStatus(object, 1, false, "", pods, podNames)
}

func statusWithNoTrackedPods(object *batchv1.Job) JobStatus {
	return NewJobStatus(object, 1, false, "", map[string]pod.PodStatus{}, nil)
}

func containsMessage(messages []string, substr string) bool {
	for _, m := range messages {
		if strings.Contains(m, substr) {
			return true
		}
	}
	return false
}

// --- NonIndexed / legacy regression -------------------------------------------------

func TestNewJobStatus_NonIndexedKeepsLegacyBehavior(t *testing.T) {
	completions := int32(3)
	object := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Name: "legacy"},
		Spec:       batchv1.JobSpec{Completions: &completions, CompletionMode: nonIndexedMode()},
		Status:     batchv1.JobStatus{Succeeded: 2},
	}

	status := statusWithNoTrackedPods(object)

	assert.Nil(t, status.Indexed, "NonIndexed jobs must not expose index interpretation")
	assert.False(t, status.IsSucceeded)
	assert.False(t, status.IsFailed)
	assert.Equal(t, int32(2), status.SucceededIndicator.Value)
	assert.Equal(t, int32(3), status.SucceededIndicator.TargetValue)
	assert.Contains(t, status.WaitingForMessages, "succeeded 2->3")
	assert.Contains(t, status.WaitingForMessages, "condition Complete->True")
}

func TestNewJobStatus_NilCompletionModeDefaultsToNonIndexed(t *testing.T) {
	completions := int32(2)
	object := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Name: "legacy-default"},
		Spec:       batchv1.JobSpec{Completions: &completions},
		Status: batchv1.JobStatus{Succeeded: 2, Conditions: []batchv1.JobCondition{
			condition(batchv1.JobComplete, corev1.ConditionTrue, batchv1.JobReasonCompletionsReached),
		}},
	}

	status := statusWithNoTrackedPods(object)

	assert.Nil(t, status.Indexed)
	assert.True(t, status.IsSucceeded)
	assert.False(t, status.IsFailed)
	assert.True(t, status.SucceededIndicator.IsReady())
}

func TestNewJobStatus_WorkQueueJobWithNilCompletions(t *testing.T) {
	parallelism := int32(3)
	object := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Name: "queue"},
		Spec: batchv1.JobSpec{
			Parallelism:    &parallelism,
			CompletionMode: nonIndexedMode(),
		},
		Status: batchv1.JobStatus{Succeeded: 0},
	}

	status := statusWithNoTrackedPods(object)

	assert.Nil(t, status.Indexed)
	assert.Equal(t, int32(1), status.SucceededIndicator.TargetValue)
	assert.Contains(t, status.WaitingForMessages, "succeeded 0->1 of 3")
}

func TestNewJobStatus_LegacyFailureReasonPassedThrough(t *testing.T) {
	completions := int32(1)
	object := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Name: "fail"},
		Spec:       batchv1.JobSpec{Completions: &completions, CompletionMode: nonIndexedMode()},
		Status: batchv1.JobStatus{
			Conditions: []batchv1.JobCondition{
				condition(batchv1.JobFailed, corev1.ConditionTrue, batchv1.JobReasonBackoffLimitExceeded),
			},
		},
	}

	status := statusWithNoTrackedPods(object)

	assert.True(t, status.IsFailed)
	assert.False(t, status.IsSucceeded)
	assert.Equal(t, batchv1.JobReasonBackoffLimitExceeded, status.FailedReason)
}

// --- Indexed: basic interpretation ---------------------------------------------------

func TestNewJobStatus_IndexedInProgress(t *testing.T) {
	// Two distinct completed indexes, but the raw succeeded pod counter is 3
	// because index 1 retried once. The indicator must follow the indexes.
	object := newIndexedJob("0,1", nil)
	object.Status.Succeeded = 3

	status := statusWithNoTrackedPods(object)

	require.NotNil(t, status.Indexed)
	assert.Equal(t, int32(2), status.Indexed.CompletedCount)
	assert.Equal(t, int32(0), status.Indexed.FailedCount)
	assert.False(t, status.Indexed.FailedIndexesReported)
	assert.Equal(t, int32(3), status.Indexed.RemainingCount)
	assert.Empty(t, status.Indexed.Warnings)

	assert.Equal(t, int32(2), status.SucceededIndicator.Value, "indicator counts distinct indexes, not succeeded pods")
	assert.Equal(t, int32(5), status.SucceededIndicator.TargetValue)
	assert.False(t, status.SucceededIndicator.IsReady())
	assert.False(t, status.IsSucceeded)
	assert.False(t, status.IsFailed)

	assert.Contains(t, status.WaitingForMessages, "completed indexes 2/5, remaining 3 [0,1]")
}

func TestNewJobStatus_IndexedNormalizationOrderIndependent(t *testing.T) {
	completions := int32(13)
	build := func(raw string) *batchv1.Job {
		return &batchv1.Job{
			ObjectMeta: metav1.ObjectMeta{Name: "idx"},
			Spec:       batchv1.JobSpec{Completions: &completions, CompletionMode: indexedMode()},
			Status:     batchv1.JobStatus{CompletedIndexes: raw},
		}
	}

	a := statusWithNoTrackedPods(build("1,3-5,7,8,10-12"))
	b := statusWithNoTrackedPods(build("10-12,8,7,3-5,1"))
	c := statusWithNoTrackedPods(build("3,1,4,5,7,8,10,11,12"))

	for _, s := range []JobStatus{a, b, c} {
		require.NotNil(t, s.Indexed)
		assert.Equal(t, int32(9), s.Indexed.CompletedCount)
		assert.Equal(t, "1,3-5,7,8,10-12", s.Indexed.Completed.String())
	}
	assert.Equal(t, a.WaitingForMessages, b.WaitingForMessages)
	assert.Equal(t, b.WaitingForMessages, c.WaitingForMessages)
}

// --- Indexed: retries / duplicate events --------------------------------------------

func TestNewJobStatus_IndexedRetriesDoNotDoubleCount(t *testing.T) {
	// The same index appears through multiple pods over time; the controller
	// string carries it once, and duplicated/overlapping tokens collapse.
	for _, raw := range []string{"2", "2,2", "2-2,2"} {
		object := newIndexedJob(raw, nil)
		status := statusWithNoTrackedPods(object)
		require.NotNil(t, status.Indexed)
		assert.Equal(t, int32(1), status.Indexed.CompletedCount, "raw=%q", raw)
		assert.Equal(t, int32(4), status.Indexed.RemainingCount, "raw=%q", raw)
	}
}

func TestNewJobStatus_PodSuccessCountNeverDrivesIndexCompletion(t *testing.T) {
	// 10 succeeded pods (heavy retries) but only 2 distinct completed indexes:
	// the Job is not complete and the indicator shows 2/5, not 10/5.
	object := newIndexedJob("0,1", nil)
	object.Status.Succeeded = 10

	pods := map[string]pod.PodStatus{}
	names := []string{"job-0-aaa", "job-0-bbb", "job-1-ccc", "job-1-ddd"}
	for _, n := range names {
		pods[n] = terminatedPod(n, true)
	}
	status := NewJobStatus(object, 1, false, "", pods, names)

	assert.Equal(t, int32(2), status.SucceededIndicator.Value)
	assert.False(t, status.SucceededIndicator.IsReady())
	assert.False(t, status.IsSucceeded, "pod names or pod count must not imply all indexes completed")
}

// --- Indexed: per-index failures -----------------------------------------------------

func TestNewJobStatus_IndexedPerIndexFailuresNotTerminal(t *testing.T) {
	object := newIndexedJob("0,1", strPtr("2"))
	object.Status.Failed = 3 // retries inflate the pod-level failed counter

	status := statusWithNoTrackedPods(object)

	require.NotNil(t, status.Indexed)
	assert.True(t, status.Indexed.FailedIndexesReported)
	assert.Equal(t, int32(2), status.Indexed.CompletedCount)
	assert.Equal(t, int32(1), status.Indexed.FailedCount)
	assert.Equal(t, int32(2), status.Indexed.RemainingCount)
	assert.False(t, status.IsFailed, "per-index failure without a terminal Failed condition is not a Job failure")
	assert.False(t, status.IsSucceeded)
	assert.Contains(t, status.WaitingForMessages, "completed indexes 2/5, failed indexes 1, remaining 2 [0,1]")
}

func TestNewJobStatus_IndexedFailureTargetIsTransitional(t *testing.T) {
	object := newIndexedJob("0,1", strPtr("2"),
		condition(batchv1.JobFailureTarget, corev1.ConditionTrue, batchv1.JobReasonMaxFailedIndexesExceeded))

	status := statusWithTerminatedPods(t, object, "p1")

	assert.False(t, status.IsFailed, "FailureTarget is transitional, not terminal")
	assert.False(t, status.IsSucceeded)
	require.NotNil(t, status.Indexed)
	require.NotEmpty(t, status.Indexed.Warnings)
	assert.Contains(t, status.Indexed.Warnings[0], "transitional condition FailureTarget=True")
}

func TestNewJobStatus_IndexedFailureTargetThenTerminalFailed(t *testing.T) {
	object := newIndexedJob("0,1", strPtr("2"),
		condition(batchv1.JobFailureTarget, corev1.ConditionFalse, ""),
		condition(batchv1.JobFailed, corev1.ConditionTrue, batchv1.JobReasonMaxFailedIndexesExceeded))

	status := statusWithTerminatedPods(t, object, "p1")

	assert.True(t, status.IsFailed)
	assert.False(t, status.IsSucceeded)
	assert.Equal(t, batchv1.JobReasonMaxFailedIndexesExceeded, status.FailedReason)
}

// --- Indexed: successPolicy ----------------------------------------------------------

func TestNewJobStatus_SuccessCriteriaMetIsTransitional(t *testing.T) {
	// successPolicy satisfied with only 3 of 5 indexes; lingering pods may
	// still be exiting, so this is not terminal yet.
	object := newIndexedJob("0,1,2", nil,
		condition(batchv1.JobSuccessCriteriaMet, corev1.ConditionTrue, batchv1.JobReasonSuccessPolicy))
	object.Spec.SuccessPolicy = &batchv1.SuccessPolicy{
		Rules: []batchv1.SuccessPolicyRule{{SucceededCount: int32Ptr(3)}},
	}

	status := statusWithNoTrackedPods(object)

	assert.False(t, status.IsSucceeded, "SuccessCriteriaMet is transitional until Complete arrives")
	assert.False(t, status.IsFailed)
	require.NotNil(t, status.Indexed)
	require.NotEmpty(t, status.Indexed.Warnings)
	assert.Contains(t, status.Indexed.Warnings[0], "transitional condition SuccessCriteriaMet=True")
	assert.Equal(t, int32(3), status.SucceededIndicator.Value)
}

func TestNewJobStatus_SuccessPolicyReachesTerminalComplete(t *testing.T) {
	object := newIndexedJob("0,1,2", nil,
		condition(batchv1.JobSuccessCriteriaMet, corev1.ConditionTrue, batchv1.JobReasonSuccessPolicy),
		condition(batchv1.JobComplete, corev1.ConditionTrue, batchv1.JobReasonSuccessPolicy))
	object.Spec.SuccessPolicy = &batchv1.SuccessPolicy{
		Rules: []batchv1.SuccessPolicyRule{{SucceededCount: int32Ptr(3)}},
	}

	status := statusWithTerminatedPods(t, object, "p1")

	assert.True(t, status.IsSucceeded)
	assert.False(t, status.IsFailed)
	// Partial success is a real success: the indicator reflects actual
	// completed indexes and must not be forced to spec.completions.
	assert.Equal(t, int32(3), status.SucceededIndicator.Value)
	assert.Equal(t, int32(5), status.SucceededIndicator.TargetValue)
}

// --- Indexed: terminal outcomes ------------------------------------------------------

func TestNewJobStatus_IndexedAllComplete(t *testing.T) {
	object := newIndexedJob("0-4", nil,
		condition(batchv1.JobComplete, corev1.ConditionTrue, batchv1.JobReasonCompletionsReached))

	status := statusWithTerminatedPods(t, object, "p0", "p1", "p2", "p3", "p4")

	assert.True(t, status.IsSucceeded)
	assert.False(t, status.IsFailed)
	require.NotNil(t, status.Indexed)
	assert.Equal(t, int32(5), status.Indexed.CompletedCount)
	assert.Equal(t, int32(0), status.Indexed.RemainingCount)
	assert.True(t, status.SucceededIndicator.IsReady())
}

func TestNewJobStatus_IndexedTerminalFailedWithUnknownReasonPreserved(t *testing.T) {
	object := newIndexedJob("0", strPtr("1"),
		condition(batchv1.JobFailed, corev1.ConditionTrue, "CustomControllerReason"))

	status := statusWithNoTrackedPods(object)

	assert.True(t, status.IsFailed)
	assert.Equal(t, "CustomControllerReason", status.FailedReason, "unknown reasons must be preserved verbatim")
}

func TestNewJobStatus_IndexedDeadlineExceededReason(t *testing.T) {
	object := newIndexedJob("0", nil,
		condition(batchv1.JobFailed, corev1.ConditionTrue, batchv1.JobReasonDeadlineExceeded))

	status := statusWithNoTrackedPods(object)
	assert.True(t, status.IsFailed)
	assert.Equal(t, batchv1.JobReasonDeadlineExceeded, status.FailedReason)
}

// --- Indexed: conflict between complete and failed sets ------------------------------

func TestNewJobStatus_OverlappingCompletedAndFailedIndexes(t *testing.T) {
	object := newIndexedJob("0-2", strPtr("2-3"))

	status := statusWithNoTrackedPods(object)

	require.NotNil(t, status.Indexed)
	require.NotEmpty(t, status.Indexed.Warnings)
	assert.Contains(t, status.Indexed.Warnings[0], "overlap")
	// Counts remain as reported; no index is silently reassigned.
	assert.Equal(t, int32(3), status.Indexed.CompletedCount)
	assert.Equal(t, int32(2), status.Indexed.FailedCount)
}

func TestNewJobStatus_ConflictingTerminalConditions(t *testing.T) {
	object := newIndexedJob("0-4", nil,
		condition(batchv1.JobComplete, corev1.ConditionTrue, ""),
		condition(batchv1.JobFailed, corev1.ConditionTrue, batchv1.JobReasonBackoffLimitExceeded))

	status := statusWithTerminatedPods(t, object, "p1")

	assert.True(t, status.IsSucceeded, "conflict is surfaced, not hidden")
	assert.True(t, status.IsFailed)
	require.NotNil(t, status.Indexed)
	assert.Contains(t, status.Indexed.Warnings[0], "conflicting terminal conditions")
	assert.Equal(t, batchv1.JobReasonBackoffLimitExceeded, status.FailedReason)
}

// --- Indexed: missing / malformed fields ---------------------------------------------

func TestNewJobStatus_MissingIndexFieldsDegradesGracefully(t *testing.T) {
	// Old cluster: completionMode Indexed but no index strings written at all.
	completions := int32(3)
	object := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Name: "old-cluster"},
		Spec:       batchv1.JobSpec{Completions: &completions, CompletionMode: indexedMode()},
		Status:     batchv1.JobStatus{Succeeded: 1},
	}

	status := statusWithNoTrackedPods(object)

	require.NotNil(t, status.Indexed)
	assert.False(t, status.Indexed.CompletedIndexesReported)
	assert.False(t, status.Indexed.FailedIndexesReported)
	assert.Equal(t, int32(0), status.Indexed.CompletedCount)
	assert.Equal(t, int32(-1), status.Indexed.RemainingCount, "unknown indexes must not be derived when no index field was reported")
	// Empty completedIndexes parses fine, so the indicator legitimately shows 0;
	// no warning is raised for an honest absence of data.
	assert.Equal(t, int32(0), status.SucceededIndicator.Value)
	assert.Empty(t, status.Indexed.Warnings)
}

func TestNewJobStatus_MalformedCompletedIndexesFallsBackWithoutSilentSuccess(t *testing.T) {
	object := newIndexedJob("0,,2", nil) // empty element -> parse error
	object.Status.Succeeded = 2

	status := statusWithNoTrackedPods(object)

	require.NotNil(t, status.Indexed)
	require.NotEmpty(t, status.Indexed.Warnings)
	assert.Contains(t, status.Indexed.Warnings[0], "invalid completedIndexes")
	// Fallback indicator uses the pod counter but the malformed data stays visible.
	assert.Equal(t, int32(2), status.SucceededIndicator.Value)
	assert.True(t, containsMessage(status.WaitingForMessages, "completed indexes could not be interpreted"))
	assert.False(t, status.IsSucceeded)
	// Remaining count cannot be derived from unparseable data.
	assert.Equal(t, int32(-1), status.Indexed.RemainingCount)
}

func TestNewJobStatus_MalformedFailedIndexesKeepsCompletedInformation(t *testing.T) {
	object := newIndexedJob("0,1", strPtr("99")) // 99 outside [0,4]
	object.Status.Failed = 1

	status := statusWithNoTrackedPods(object)

	require.NotNil(t, status.Indexed)
	assert.Equal(t, int32(2), status.Indexed.CompletedCount, "valid completed data is retained")
	assert.Contains(t, status.Indexed.Warnings[0], "invalid failedIndexes")
	assert.True(t, status.Indexed.FailedIndexesReported)
}

// --- Indexed: unterminated pods gate condition checks --------------------------------

func TestNewJobStatus_UnterminatedPodsBlockConditionEvaluation(t *testing.T) {
	object := newIndexedJob("0-4", nil,
		condition(batchv1.JobComplete, corev1.ConditionTrue, ""))

	running := pod.PodStatus{Name: "running", StatusIndicator: &indicators.StringEqualConditionIndicator{}}
	done := terminatedPod("done", true)
	pods := map[string]pod.PodStatus{"running": running, "done": done}

	status := NewJobStatus(object, 1, false, "", pods, []string{"running", "done"})

	assert.False(t, status.IsSucceeded, "a lagging pod snapshot blocks terminal verdicts")
	assert.Contains(t, status.WaitingForMessages, "pods should be terminated")
}

func TestNewJobStatus_StalePodSnapshotDoesNotOverrideNewerComplete(t *testing.T) {
	// The controller already wrote Complete, but a cached pod still looks
	// active. Once all tracked pods terminate the controller result wins.
	object := newIndexedJob("0-4", nil,
		condition(batchv1.JobComplete, corev1.ConditionTrue, ""))

	status := statusWithTerminatedPods(t, object, "stale-pod")
	assert.True(t, status.IsSucceeded)
}

func TestNewJobStatus_NonTrueConditionsAreIgnored(t *testing.T) {
	// A Failed condition with status False or Unknown is a transient probe
	// result, not a failure; only ConditionTrue carries the verdict.
	object := newIndexedJob("0,1", nil,
		condition(batchv1.JobFailed, corev1.ConditionFalse, batchv1.JobReasonBackoffLimitExceeded),
		condition(batchv1.JobComplete, corev1.ConditionUnknown, ""))

	status := statusWithNoTrackedPods(object)

	assert.False(t, status.IsFailed)
	assert.False(t, status.IsSucceeded)
	assert.Empty(t, status.FailedReason)
}

func TestNewJobStatus_PerIndexFailureWaitingMessageShape(t *testing.T) {
	object := newIndexedJob("0,2,4", strPtr("1"))
	// completions=5: completed 0,2,4; failed 1; remaining index 3.

	status := statusWithNoTrackedPods(object)

	var indexMessage string
	for _, m := range status.WaitingForMessages {
		if strings.HasPrefix(m, "completed indexes ") {
			indexMessage = m
		}
	}
	require.NotEmpty(t, indexMessage)
	assert.Equal(t, "completed indexes 3/5, failed indexes 1, remaining 1 [0,2,4]", indexMessage)
}

// --- Bounded summary -----------------------------------------------------------------

func TestNewJobStatus_IndexedSummaryIsBounded(t *testing.T) {
	completions := int32(1000001)
	object := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Name: "huge"},
		Spec:       batchv1.JobSpec{Completions: &completions, CompletionMode: indexedMode()},
		// Only a prefix completed so the indicator is not ready and the
		// bounded summary is actually emitted into WaitingForMessages.
		Status: batchv1.JobStatus{CompletedIndexes: "0-999999"},
	}

	status := statusWithNoTrackedPods(object)

	var summary string
	for _, m := range status.WaitingForMessages {
		if strings.HasPrefix(m, "completed indexes ") {
			summary = m
		}
	}
	require.NotEmpty(t, summary)
	assert.LessOrEqual(t, len(summary), maxSummaryChars+40) // suffix allowance
	assert.Contains(t, summary, "0-999999")
	assert.NotContains(t, summary, "999999,999998") // never expanded index by index
}
