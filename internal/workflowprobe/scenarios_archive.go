package workflowprobe

import (
	"fmt"
	"net/http"

	"solo-0014-zone-rollout-control/internal/domain"
)

// runPlanArchive drives a completed plan (including revision, lease, probe and
// assessment history) through archive idempotency, scope-filtered listing,
// history-preserving reads and restore over the real HTTP surface.
func runPlanArchive() error {
	harness, err := newHarness()
	if err != nil {
		return err
	}
	defer harness.close()

	if _, err := enrollNode(harness, "resolver-archive-01", "north"); err != nil {
		return err
	}
	plan, revision, err := createActivePlan(harness, "archive.example", 5001, 1, 1, 40)
	if err != nil {
		return err
	}
	group, err := allocate(harness, plan.ID, "archive-lease-0001", "north", 1)
	if err != nil {
		return err
	}
	if _, err := submitProbe(harness, group.LeaseIDs[0], "archive-probe-0001", true, revision.Serial, 12); err != nil {
		return err
	}
	var assessment domain.RolloutAssessment
	if err := harness.call(http.MethodPatch, "/v1/plans/"+plan.ID+"/assessments", domain.AssessmentInput{
		RequestKey: "archive-assess-0001",
	}, &assessment); err != nil {
		return err
	}

	// Assessment bumps the plan to version 4; completion moves it to version 5.
	if err := harness.call(http.MethodPatch, "/v1/plans/"+plan.ID+"/complete", map[string]any{
		"expected_version": plan.Version + 1,
	}, &plan); err != nil {
		return err
	}
	if plan.State != domain.PlanCompleted || plan.Version != 5 {
		return ensure(false, "archive candidate was not completed at version 5")
	}

	other, _, err := createActivePlan(harness, "other.example", 6001, 1, 1, 40)
	if err != nil {
		return err
	}
	if err := harness.call(http.MethodPatch, "/v1/plans/"+other.ID+"/complete", map[string]any{
		"expected_version": other.Version,
	}, &other); err != nil {
		return err
	}
	var draftPlan domain.RolloutPlan
	if err := harness.call(http.MethodPut, "/v1/plans", domain.PlanCreateInput{
		Zone:                "draft.example",
		MinimumSuccessRatio: 1,
		MaximumLatencyMS:    40,
		ExpectedNodes:       1,
	}, &draftPlan); err != nil {
		return err
	}

	// Active or draft plans cannot be archived.
	err = harness.call(http.MethodPatch, "/v1/plans/"+draftPlan.ID+"/archive", map[string]any{
		"expected_version": draftPlan.Version,
	}, nil)
	if err := expectFailure(err, http.StatusConflict, string(domain.CodeInvalidState)); err != nil {
		return err
	}

	// First archive submits a new plan version.
	var archived domain.RolloutPlan
	if err := harness.call(http.MethodPatch, "/v1/plans/"+plan.ID+"/archive", map[string]any{
		"expected_version": plan.Version,
	}, &archived); err != nil {
		return err
	}
	if !archived.Archived || archived.ArchivedAt == nil || archived.Version != 6 {
		return ensure(false, "plan was not archived at a new version")
	}
	if archived.State != domain.PlanCompleted {
		return ensure(false, "archiving changed the plan lifecycle state")
	}
	if archived.LatestRevisionID != revision.ID || archived.LatestAssessmentID != assessment.ID {
		return ensure(false, "archiving dropped revision or assessment references")
	}

	// Repeated archiving is idempotent and returns the same aggregate, even when
	// the client replays a stale expected version.
	var replay domain.RolloutPlan
	if err := harness.call(http.MethodPatch, "/v1/plans/"+plan.ID+"/archive", map[string]any{
		"expected_version": archived.Version,
	}, &replay); err != nil {
		return err
	}
	if replay.Version != archived.Version || !replay.Archived || replay.ArchivedAt == nil {
		return ensure(false, "archive replay was not idempotent")
	}
	var staleReplay domain.RolloutPlan
	if err := harness.call(http.MethodPatch, "/v1/plans/"+plan.ID+"/archive", map[string]any{
		"expected_version": 1,
	}, &staleReplay); err != nil {
		return err
	}
	if staleReplay.Version != archived.Version || !staleReplay.Archived {
		return ensure(false, "archive replay with a stale version was not idempotent")
	}

	// Archived plans are hidden from the default list but available through
	// explicit archive scopes and state filters.
	defaultPage, err := listPlans(harness, "limit=100")
	if err != nil {
		return err
	}
	if hasPlan(defaultPage.Items, plan.ID) {
		return ensure(false, "archived plan appeared in the default list")
	}
	if !hasPlan(defaultPage.Items, other.ID) || !hasPlan(defaultPage.Items, draftPlan.ID) {
		return ensure(false, "default list lost visible plans")
	}
	archivedPage, err := listPlans(harness, "scope=archived&limit=100")
	if err != nil {
		return err
	}
	if !hasPlan(archivedPage.Items, plan.ID) || hasPlan(archivedPage.Items, other.ID) {
		return ensure(false, "archived scope did not return only archived plans")
	}
	allPage, err := listPlans(harness, "scope=all&limit=100")
	if err != nil {
		return err
	}
	for _, id := range []string{plan.ID, other.ID, draftPlan.ID} {
		if !hasPlan(allPage.Items, id) {
			return ensure(false, "all scope is missing plan "+id)
		}
	}
	completedVisible, err := listPlans(harness, "state=completed&limit=100")
	if err != nil {
		return err
	}
	if hasPlan(completedVisible.Items, plan.ID) || !hasPlan(completedVisible.Items, other.ID) {
		return ensure(false, "state filter did not keep the default archive scope")
	}
	completedArchived, err := listPlans(harness, "state=completed&scope=archived&limit=100")
	if err != nil {
		return err
	}
	if !hasPlan(completedArchived.Items, plan.ID) {
		return ensure(false, "state and archive filters could not be combined")
	}
	draftArchived, err := listPlans(harness, "state=draft&scope=archived&limit=100")
	if err != nil {
		return err
	}
	if len(draftArchived.Items) != 0 {
		return ensure(false, "combined filters returned unexpected plans")
	}
	err = harness.call(http.MethodGet, "/v1/plans?scope=bogus", nil, nil)
	if err := expectFailure(err, http.StatusBadRequest, string(domain.CodeValidationFailed)); err != nil {
		return err
	}

	// Completed reads keep working and history remains referenced.
	var detail domain.RolloutPlan
	if err := harness.call(http.MethodGet, "/v1/plans/"+plan.ID, nil, &detail); err != nil {
		return err
	}
	if !detail.Archived || detail.LatestRevisionID != revision.ID || detail.LatestAssessmentID != assessment.ID {
		return ensure(false, "archived plan detail lost status or history references")
	}
	var latest domain.RolloutAssessment
	if err := harness.call(http.MethodGet, "/v1/plans/"+plan.ID+"/assessments/latest", nil, &latest); err != nil {
		return err
	}
	if latest.ID != assessment.ID {
		return ensure(false, "archived plan no longer exposes its latest assessment")
	}

	// Restore checks the version and submits a new plan version.
	err = harness.call(http.MethodPatch, "/v1/plans/"+plan.ID+"/restore", map[string]any{
		"expected_version": 5,
	}, nil)
	if err := expectFailure(err, http.StatusConflict, string(domain.CodeVersionConflict)); err != nil {
		return err
	}
	var restored domain.RolloutPlan
	if err := harness.call(http.MethodPatch, "/v1/plans/"+plan.ID+"/restore", map[string]any{
		"expected_version": archived.Version,
	}, &restored); err != nil {
		return err
	}
	if restored.Archived || restored.ArchivedAt != nil || restored.RestoredAt == nil || restored.Version != 7 {
		return ensure(false, "restore did not clear archive status at a new version")
	}
	if restored.LatestRevisionID != revision.ID || restored.LatestAssessmentID != assessment.ID {
		return ensure(false, "restore dropped revision or assessment references")
	}
	err = harness.call(http.MethodPatch, "/v1/plans/"+plan.ID+"/restore", map[string]any{
		"expected_version": restored.Version,
	}, nil)
	if err := expectFailure(err, http.StatusConflict, string(domain.CodeInvalidState)); err != nil {
		return err
	}
	restoredDefault, err := listPlans(harness, "limit=100")
	if err != nil {
		return err
	}
	if !hasPlan(restoredDefault.Items, plan.ID) {
		return ensure(false, "restored plan did not return to the default list")
	}
	restoredArchived, err := listPlans(harness, "scope=archived&limit=100")
	if err != nil {
		return err
	}
	if hasPlan(restoredArchived.Items, plan.ID) {
		return ensure(false, "restored plan remained in the archived scope")
	}

	// A restored completed plan can be archived again.
	var reArchived domain.RolloutPlan
	if err := harness.call(http.MethodPatch, "/v1/plans/"+plan.ID+"/archive", map[string]any{
		"expected_version": restored.Version,
	}, &reArchived); err != nil {
		return err
	}
	if !reArchived.Archived || reArchived.Version != 8 {
		return ensure(false, "re-archive after restore did not produce another version")
	}

	// Existing pagination stays usable alongside the archive scope.
	seen := make(map[string]struct{})
	cursor := ""
	for {
		query := "scope=all&limit=1"
		if cursor != "" {
			query += "&cursor=" + cursor
		}
		page, err := listPlans(harness, query)
		if err != nil {
			return err
		}
		if len(page.Items) != 1 {
			return ensure(false, fmt.Sprintf("expected one item per page, got %d", len(page.Items)))
		}
		if _, duplicated := seen[page.Items[0].ID]; duplicated {
			return ensure(false, "pagination returned a duplicated plan")
		}
		seen[page.Items[0].ID] = struct{}{}
		if page.Meta.NextCursor == "" {
			break
		}
		cursor = page.Meta.NextCursor
	}
	for _, id := range []string{plan.ID, other.ID, draftPlan.ID} {
		if _, ok := seen[id]; !ok {
			return ensure(false, "pagination missed plan "+id)
		}
	}
	return nil
}

func listPlans(harness *harness, rawQuery string) (domain.PlanPage, error) {
	var page domain.PlanPage
	err := harness.call(http.MethodGet, "/v1/plans?"+rawQuery, nil, &page)
	return page, err
}

func hasPlan(items []domain.RolloutPlan, id string) bool {
	for _, item := range items {
		if item.ID == id {
			return true
		}
	}
	return false
}
