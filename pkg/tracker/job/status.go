package job

import (
	"fmt"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"

	"github.com/werf/kubedog/pkg/tracker/indicators"
	"github.com/werf/kubedog/pkg/tracker/pod"
	"github.com/werf/kubedog/pkg/utils"
)

// Bounds for index summaries emitted into waiting messages. The raw controller
// strings may encode millions of indexes; these caps keep log lines bounded
// while preserving the leading ranges and the omitted count.
const (
	maxSummaryRanges = 8
	maxSummaryChars  = 120
)

// IndexedStatus is the interpreted per-index outcome of an Indexed Job.
//
// All counts and sets here are derived exclusively from the controller-written
// Job status (completedIndexes / failedIndexes), never from pod names or the
// number of succeeded pods: retries create multiple Pods for one index, so the
// pod success count cannot be treated as the number of completed indexes.
// Fields stay zero-valued when the corresponding controller field is absent,
// so older clusters that do not write a field degrade without guessing.
type IndexedStatus struct {
	// Completed holds the normalized set parsed from status.completedIndexes.
	Completed IndexSet
	// Failed holds the normalized set parsed from status.failedIndexes.
	Failed IndexSet

	// CompletedIndexesReported is false when status.completedIndexes is absent.
	// An empty string is indistinguishable from a missing field on the wire, so
	// when neither index field is reported the remaining count stays unknown
	// rather than being derived from spec.completions alone.
	CompletedIndexesReported bool
	// FailedIndexesReported is false when status.failedIndexes is nil, i.e.
	// the controller (or cluster version) did not report per-index failures.
	FailedIndexesReported bool

	// CompletedCount / FailedCount are the distinct-index counts. RemainingCount
	// is the number of indexes neither completed nor failed relative to
	// spec.completions; it is -1 when it cannot be derived.
	CompletedCount int32
	FailedCount    int32
	RemainingCount int32

	// Warnings carries identifiable inconsistencies detected while interpreting
	// the controller status (unparseable fields, overlapping sets, conflicting
	// conditions). Warnings never change terminal verdicts; they are surfaced
	// through WaitingForMessages so unknown state stays visible.
	Warnings []string

	// parseFailed is true when at least one controller-written index field could
	// not be parsed or validated. Transitional-condition warnings do not set it.
	parseFailed bool
}

type JobStatus struct {
	batchv1.JobStatus

	StatusGeneration uint64

	SucceededIndicator *indicators.Int32EqualConditionIndicator
	Age                string

	WaitingForMessages []string

	IsSucceeded  bool
	IsFailed     bool
	FailedReason string

	Pods map[string]pod.PodStatus

	// Indexed is non-nil only for Jobs whose spec.completionMode is Indexed.
	// For NonIndexed Jobs and work-queue Jobs (nil completions) the field stays
	// nil and all legacy behavior is preserved.
	Indexed *IndexedStatus
}

func NewJobStatus(object *batchv1.Job, statusGeneration uint64, isTrackerFailed bool, trackerFailedReason string, podsStatuses map[string]pod.PodStatus, trackedPodsNames []string) JobStatus {
	res := JobStatus{
		JobStatus:        object.Status,
		StatusGeneration: statusGeneration,
		Age:              utils.TranslateTimestampSince(object.CreationTimestamp),
		Pods:             make(map[string]pod.PodStatus),
	}

	for k, v := range podsStatuses {
		res.Pods[k] = v
		if v.StatusIndicator != nil {
			v.StatusIndicator.TargetValue = "Completed"
		}
	}

	isIndexed := object.Spec.CompletionMode != nil && *object.Spec.CompletionMode == batchv1.IndexedCompletion
	if isIndexed {
		res.Indexed = interpretIndexedStatus(object)
	}

	doCheckJobConditions := true
	for _, trackedPodName := range trackedPodsNames {
		podStatus := podsStatuses[trackedPodName]

		if !podStatus.IsFailed && !podStatus.IsSucceeded {
			doCheckJobConditions = false // unterminated pods exists
		}
	}

	if doCheckJobConditions {
		evaluateJobConditions(object, &res)

		if !res.IsSucceeded {
			res.WaitingForMessages = append(res.WaitingForMessages, fmt.Sprintf("condition %s->%s", batchv1.JobComplete, corev1.ConditionTrue))
		}
	} else {
		res.WaitingForMessages = append(res.WaitingForMessages, "pods should be terminated")
	}

	res.SucceededIndicator = &indicators.Int32EqualConditionIndicator{}

	switch {
	case isIndexed:
		// For Indexed Jobs the indicator tracks distinct completed indexes
		// rather than succeeded pods; fall back to the raw succeeded counter
		// only when the controller status could not be interpreted.
		indexInterpretable := res.Indexed != nil && !res.Indexed.hasParseWarnings()
		if indexInterpretable {
			res.SucceededIndicator.Value = res.Indexed.CompletedCount
		} else {
			// Keep the legacy counter usable but never pretend it is an index count.
			res.SucceededIndicator.Value = object.Status.Succeeded
			if res.Indexed != nil {
				res.WaitingForMessages = append(res.WaitingForMessages,
					"completed indexes could not be interpreted; falling back to status.succeeded for the progress indicator")
			}
		}

		if object.Spec.Completions != nil {
			res.SucceededIndicator.TargetValue = *object.Spec.Completions
		} else {
			// An Indexed Job without completions violates the API contract;
			// keep the indicator usable instead of dividing by zero.
			res.SucceededIndicator.TargetValue = res.SucceededIndicator.Value
		}

		if !res.SucceededIndicator.IsReady() {
			res.WaitingForMessages = append(res.WaitingForMessages, buildIndexedWaitingMessage(object, res.Indexed))
		}
		res.WaitingForMessages = append(res.WaitingForMessages, res.Indexed.Warnings...)

	case object.Spec.Completions != nil:
		res.SucceededIndicator.Value = object.Status.Succeeded
		res.SucceededIndicator.TargetValue = *object.Spec.Completions

		if !res.SucceededIndicator.IsReady() {
			res.WaitingForMessages = append(res.WaitingForMessages, fmt.Sprintf("succeeded %d->%d", res.SucceededIndicator.Value, res.SucceededIndicator.TargetValue))
		}
	default:
		res.SucceededIndicator.Value = object.Status.Succeeded
		res.SucceededIndicator.TargetValue = 1

		if !res.IsSucceeded {
			parallelism := int32(0)
			if object.Spec.Parallelism != nil {
				parallelism = *object.Spec.Parallelism
			}
			if parallelism > 1 {
				res.WaitingForMessages = append(res.WaitingForMessages, fmt.Sprintf("succeeded %d->%d of %d", res.SucceededIndicator.Value, res.SucceededIndicator.TargetValue, parallelism))
			} else {
				res.WaitingForMessages = append(res.WaitingForMessages, fmt.Sprintf("succeeded %d->%d", res.SucceededIndicator.Value, res.SucceededIndicator.TargetValue))
			}
		}
	}

	if !res.IsSucceeded && !res.IsFailed {
		res.IsFailed = isTrackerFailed
		res.FailedReason = trackerFailedReason
	}

	return res
}

// evaluateJobConditions derives terminal verdicts solely from controller
// conditions. The transitional FailureTarget and SuccessCriteriaMet conditions
// are intentionally not terminal: lingering pods may still be exiting when they
// appear, and the final outcome arrives later as Complete or Failed.
func evaluateJobConditions(object *batchv1.Job, res *JobStatus) {
	var completeTrue, failedTrue, failureTargetTrue, successCriteriaTrue bool
	var failedReason string

	for _, c := range object.Status.Conditions {
		if c.Status != corev1.ConditionTrue {
			continue
		}

		switch c.Type {
		case batchv1.JobComplete:
			completeTrue = true
		case batchv1.JobFailed:
			failedTrue = true
			// Preserve the controller reason verbatim, including reasons other
			// than BackoffLimitExceeded (DeadlineExceeded, PodFailurePolicy,
			// MaxFailedIndexesExceeded, FailedIndexes, unknown custom reasons).
			failedReason = c.Reason
		case batchv1.JobFailureTarget:
			failureTargetTrue = true
		case batchv1.JobSuccessCriteriaMet:
			successCriteriaTrue = true
		}
	}

	switch {
	case completeTrue && failedTrue:
		// The API contract says a Job cannot carry both terminal conditions.
		// Do not pick a winner: keep both verdicts visible and flag the
		// inconsistency instead of fabricating an outcome.
		res.IsSucceeded = true
		res.IsFailed = true
		res.FailedReason = failedReason
		res.appendConditionWarning(fmt.Sprintf(
			"inconsistent Job status: conflicting terminal conditions %s=True and %s=True reported by the controller",
			batchv1.JobComplete, batchv1.JobFailed))
	case completeTrue:
		res.IsSucceeded = true
	case failedTrue:
		res.IsFailed = true
		res.FailedReason = failedReason
	case failureTargetTrue:
		// Transitional: per-index failures reached a target (e.g.
		// MaxFailedIndexesExceeded) but the terminal Failed condition has not
		// been written yet. Nothing is terminal here; record progress only.
		res.appendConditionWarning(fmt.Sprintf(
			"transitional condition %s=True: waiting for terminal %s or %s",
			batchv1.JobFailureTarget, batchv1.JobComplete, batchv1.JobFailed))
	case successCriteriaTrue:
		// Transitional: successPolicy rules (or completions) are satisfied but
		// lingering pods are still being terminated; Complete comes afterwards.
		res.appendConditionWarning(fmt.Sprintf(
			"transitional condition %s=True: success criteria met, waiting for terminal %s",
			batchv1.JobSuccessCriteriaMet, batchv1.JobComplete))
	}
}

func (res *JobStatus) appendConditionWarning(msg string) {
	if res.Indexed != nil {
		res.Indexed.Warnings = append(res.Indexed.Warnings, msg)
	} else {
		res.WaitingForMessages = append(res.WaitingForMessages, msg)
	}
}

// interpretIndexedStatus parses the controller-written index sets without ever
// deriving unknown indexes: a missing or unparseable field keeps the
// information that is available and records an identifiable warning.
func interpretIndexedStatus(object *batchv1.Job) *IndexedStatus {
	out := &IndexedStatus{RemainingCount: -1}

	// completedIndexes is a plain string: presence means the controller wrote
	// the field, even when empty. Record it so callers can distinguish an old
	// cluster that reports nothing from a fresh Job with zero completions.
	out.CompletedIndexesReported = object.Status.CompletedIndexes != ""

	completed, err := ParseIndexSet("completedIndexes", object.Status.CompletedIndexes, object.Spec.Completions)
	if err != nil {
		out.parseFailed = true
		out.Warnings = append(out.Warnings, err.Error())
	} else {
		out.Completed = completed
		out.CompletedCount, err = completed.Count()
		if err != nil {
			out.parseFailed = true
			out.Warnings = append(out.Warnings, err.Error())
		}
	}

	if object.Status.FailedIndexes != nil {
		out.FailedIndexesReported = true

		failed, err := ParseIndexSet("failedIndexes", *object.Status.FailedIndexes, object.Spec.Completions)
		if err != nil {
			out.parseFailed = true
			out.Warnings = append(out.Warnings, err.Error())
		} else {
			out.Failed = failed
			out.FailedCount, err = failed.Count()
			if err != nil {
				out.parseFailed = true
				out.Warnings = append(out.Warnings, err.Error())
			}
		}
	}

	// The API documents that failed and completed index sets must not overlap.
	// If a cluster reports an overlap anyway, do not assign the shared indexes
	// to either outcome: report the inconsistency and leave the local counts
	// as parsed so the controller's terminal conditions remain the authority.
	if out.FailedIndexesReported && out.Completed.Intersects(out.Failed) {
		out.Warnings = append(out.Warnings, fmt.Sprintf(
			"inconsistent Job status: completedIndexes %q and failedIndexes %q overlap; reporting controller conditions instead of guessing index outcomes",
			object.Status.CompletedIndexes, derefString(object.Status.FailedIndexes)))
	}

	// Only derive a remaining count when the controller actually reported at
	// least one index field and parsing succeeded; otherwise the unknown
	// indexes stay unknown instead of being inferred from spec.completions.
	indexesReported := out.CompletedIndexesReported || out.FailedIndexesReported
	if object.Spec.Completions != nil && indexesReported && !out.parseFailed {
		known := int64(out.CompletedCount) + int64(out.FailedCount)
		total := int64(*object.Spec.Completions)
		if known <= total {
			out.RemainingCount = int32(total - known)
		} else {
			out.Warnings = append(out.Warnings, fmt.Sprintf(
				"inconsistent Job status: completed (%d) and failed (%d) indexes exceed spec.completions (%d)",
				out.CompletedCount, out.FailedCount, *object.Spec.Completions))
		}
	}

	return out
}

// hasParseWarnings reports whether controller-written index fields failed
// parsing or validation, in which case the index-derived indicator cannot be
// trusted and the legacy succeeded-pod counter is used instead.
func (s *IndexedStatus) hasParseWarnings() bool {
	return s.parseFailed
}

func buildIndexedWaitingMessage(object *batchv1.Job, indexed *IndexedStatus) string {
	base := fmt.Sprintf("completed indexes %d", indexed.CompletedCount)
	if object.Spec.Completions != nil {
		base = fmt.Sprintf("%s/%d", base, *object.Spec.Completions)
	}

	if indexed.FailedIndexesReported {
		base += fmt.Sprintf(", failed indexes %d", indexed.FailedCount)
	}
	if indexed.RemainingCount >= 0 {
		base += fmt.Sprintf(", remaining %d", indexed.RemainingCount)
	}

	if summary := indexed.Completed.FormatBounded(maxSummaryRanges, maxSummaryChars); summary != "" {
		base += fmt.Sprintf(" [%s]", summary)
	}

	return base
}

func derefString(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}
