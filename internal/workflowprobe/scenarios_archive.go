package workflowprobe

import (
	"fmt"
	"net/http"
	"net/url"

	"solo-0014-zone-rollout-control/internal/domain"
)

func runPlanArchiveLifecycle() error {
	harness, err := newHarness()
	if err != nil {
		return err
	}
	defer harness.close()

	// Plan A carries a lease, probe, and assessment while active so that
	// archiving can be proven to preserve every historical reference.
	planA, revision, err := createActivePlan(harness, "archive-a.example", 5101, 1, 1, 50)
	if err != nil {
		return err
	}
	if _, err := enrollNode(harness, "resolver-archive-01", "north"); err != nil {
		return err
	}
	group, err := allocate(harness, planA.ID, "archive-lease-0001", "north", 1)
	if err != nil {
		return err
	}
	if _, err := submitProbe(
		harness,
		group.LeaseIDs[0],
		"archive-probe-0001",
		true,
		revision.Serial,
		10,
	); err != nil {
		return err
	}
	var assessment domain.RolloutAssessment
	if err := harness.call(http.MethodPatch, "/v1/plans/"+planA.ID+"/assessments", domain.AssessmentInput{
		RequestKey: "archive-assess-001",
	}, &assessment); err != nil {
		return err
	}
	if err := harness.call(http.MethodGet, "/v1/plans/"+planA.ID, nil, &planA); err != nil {
		return err
	}
	if err := completePlan(harness, &planA); err != nil {
		return err
	}

	planB, _, err := createActivePlan(harness, "archive-b.example", 5201, 1, 1, 50)
	if err != nil {
		return err
	}
	if err := completePlan(harness, &planB); err != nil {
		return err
	}

	var draft domain.RolloutPlan
	if err := harness.call(http.MethodPut, "/v1/plans", domain.PlanCreateInput{
		Zone:                "archive-c.example",
		MinimumSuccessRatio: 1,
		MaximumLatencyMS:    50,
		ExpectedNodes:       1,
	}, &draft); err != nil {
		return err
	}

	// Active or draft plans cannot be archived.
	active, _, err := createActivePlan(harness, "archive-d.example", 5401, 1, 1, 50)
	if err != nil {
		return err
	}
	if err := harness.call(http.MethodPatch, "/v1/plans/"+active.ID+"/archive", nil, nil); err != nil {
		if err := expectFailure(err, http.StatusConflict, string(domain.CodeInvalidState)); err != nil {
			return err
		}
	} else {
		return ensure(false, "active plan was archived")
	}
	if err := harness.call(http.MethodPatch, "/v1/plans/"+draft.ID+"/archive", nil, nil); err != nil {
		if err := expectFailure(err, http.StatusConflict, string(domain.CodeInvalidState)); err != nil {
			return err
		}
	} else {
		return ensure(false, "draft plan was archived")
	}

	var archived domain.RolloutPlan
	if err := harness.call(http.MethodPatch, "/v1/plans/"+planA.ID+"/archive", map[string]any{
		"expected_version": planA.Version,
	}, &archived); err != nil {
		return err
	}
	if !archived.Archived || archived.ArchivedAt == nil {
		return ensure(false, "completed plan was not archived")
	}
	if archived.Version != planA.Version+1 {
		return ensure(false, "archiving did not commit a new plan version")
	}
	if archived.LatestRevisionID == "" || archived.LatestAssessmentID != assessment.ID {
		return ensure(false, "archiving dropped revision or assessment references")
	}

	// Repeated archiving is idempotent: same version, no history mutation.
	var replayed domain.RolloutPlan
	if err := harness.call(http.MethodPatch, "/v1/plans/"+planA.ID+"/archive", map[string]any{
		"expected_version": planA.Version,
	}, &replayed); err != nil {
		return err
	}
	if replayed.Version != archived.Version || !replayed.Archived ||
		replayed.ArchivedAt == nil || *replayed.ArchivedAt != *archived.ArchivedAt {
		return ensure(false, "archive replay was not idempotent")
	}

	// A stale version on the first transition is a conflict.
	if err := harness.call(http.MethodPatch, "/v1/plans/"+planB.ID+"/archive", map[string]any{
		"expected_version": planB.Version - 1,
	}, nil); err != nil {
		if err := expectFailure(err, http.StatusConflict, string(domain.CodeVersionConflict)); err != nil {
			return err
		}
	} else {
		return ensure(false, "stale archive version was accepted")
	}
	if err := harness.call(http.MethodPatch, "/v1/plans/"+planB.ID+"/archive", nil, nil); err != nil {
		return err
	}

	// Archived plans are hidden by default and by scope=current, but an
	// explicit scope retrieves them; the state filter still applies.
	defaultIDs, err := listPlanIDs(harness, "", "")
	if err != nil {
		return err
	}
	if contains(defaultIDs, planA.ID) || contains(defaultIDs, planB.ID) {
		return ensure(false, "archived plans appeared in the default list")
	}
	if !contains(defaultIDs, active.ID) || !contains(defaultIDs, draft.ID) {
		return ensure(false, "active or draft plan disappeared from the default list")
	}

	currentIDs, err := listPlanIDs(harness, "current", "")
	if err != nil {
		return err
	}
	if contains(currentIDs, planA.ID) {
		return ensure(false, "archived plan appeared under scope=current")
	}
	archivedIDs, err := listPlanIDs(harness, "archived", "")
	if err != nil {
		return err
	}
	if !contains(archivedIDs, planA.ID) || !contains(archivedIDs, planB.ID) ||
		contains(archivedIDs, active.ID) {
		return ensure(false, "scope=archived did not return exactly the archived plans")
	}
	allIDs, err := listPlanIDs(harness, "all", "")
	if err != nil {
		return err
	}
	for _, id := range []string{planA.ID, planB.ID, active.ID, draft.ID} {
		if !contains(allIDs, id) {
			return ensure(false, "scope=all is missing a plan")
		}
	}
	completedOnly, err := listPlanIDs(harness, "archived", string(domain.PlanCompleted))
	if err != nil {
		return err
	}
	if !contains(completedOnly, planA.ID) || contains(completedOnly, active.ID) {
		return ensure(false, "state filter did not compose with archive scope")
	}
	if _, err := listPlanIDs(harness, "bogus", ""); err == nil {
		return ensure(false, "invalid archive scope was accepted")
	}

	// Detail and completed reads stay available for an archived plan.
	var detail domain.RolloutPlan
	if err := harness.call(http.MethodGet, "/v1/plans/"+planA.ID, nil, &detail); err != nil {
		return err
	}
	if !detail.Archived || detail.LatestAssessmentID != assessment.ID {
		return ensure(false, "archived plan detail lost archive state or references")
	}
	var latest domain.RolloutAssessment
	if err := harness.call(http.MethodGet, "/v1/plans/"+planA.ID+"/assessments/latest", nil, &latest); err != nil {
		return err
	}
	if latest.ID != assessment.ID {
		return ensure(false, "archived plan no longer exposes its latest assessment")
	}

	// Restore commits a version, keeps state=completed, and is idempotent.
	var restored domain.RolloutPlan
	if err := harness.call(http.MethodPatch, "/v1/plans/"+planA.ID+"/restore", map[string]any{
		"expected_version": archived.Version,
	}, &restored); err != nil {
		return err
	}
	if restored.Archived || restored.ArchivedAt != nil {
		return ensure(false, "restore did not clear archive markers")
	}
	if restored.State != domain.PlanCompleted || restored.Version != archived.Version+1 {
		return ensure(false, "restore changed lifecycle state or did not commit a version")
	}
	if restored.LatestRevisionID == "" || restored.LatestAssessmentID != assessment.ID {
		return ensure(false, "restore dropped revision or assessment references")
	}
	var restoreReplay domain.RolloutPlan
	if err := harness.call(http.MethodPatch, "/v1/plans/"+planA.ID+"/restore", nil, &restoreReplay); err != nil {
		return err
	}
	if restoreReplay.Version != restored.Version || restoreReplay.Archived {
		return ensure(false, "restore replay was not idempotent")
	}
	afterRestore, err := listPlanIDs(harness, "", "")
	if err != nil {
		return err
	}
	if !contains(afterRestore, planA.ID) {
		return ensure(false, "restored plan did not return to the default list")
	}
	stillArchived, err := listPlanIDs(harness, "archived", "")
	if err != nil {
		return err
	}
	if contains(stillArchived, planA.ID) {
		return ensure(false, "restored plan still appears as archived")
	}
	return nil
}

func completePlan(harness *harness, plan *domain.RolloutPlan) error {
	var completed domain.RolloutPlan
	if err := harness.call(http.MethodPatch, "/v1/plans/"+plan.ID+"/complete", map[string]any{
		"expected_version": plan.Version,
	}, &completed); err != nil {
		return err
	}
	*plan = completed
	return nil
}

// listPlanIDs pages through GET /v1/plans for the given archive scope and
// lifecycle state, returning every plan id in stable id order.
func listPlanIDs(harness *harness, scope string, state string) ([]string, error) {
	ids := make([]string, 0)
	cursor := ""
	for {
		query := url.Values{}
		query.Set("limit", "1")
		if scope != "" {
			query.Set("scope", scope)
		}
		if state != "" {
			query.Set("state", state)
		}
		if cursor != "" {
			query.Set("cursor", cursor)
		}
		var page domain.PlanPage
		if err := harness.call(http.MethodGet, "/v1/plans?"+query.Encode(), nil, &page); err != nil {
			return nil, fmt.Errorf("list plans: %w", err)
		}
		for _, plan := range page.Items {
			ids = append(ids, plan.ID)
		}
		cursor = page.Meta.NextCursor
		if cursor == "" {
			return ids, nil
		}
	}
}

func contains(ids []string, target string) bool {
	for _, id := range ids {
		if id == target {
			return true
		}
	}
	return false
}
