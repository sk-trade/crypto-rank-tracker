from pathlib import Path


def test_gen2_deployment_uses_explicit_limits_and_separate_service_accounts():
    workflow = Path(".github/workflows/deploy.yaml").read_text(encoding="utf-8")

    assert "GCP_DEPLOYER_SA_EMAIL" in workflow
    assert "GCP_RUNTIME_SA_EMAIL" in workflow
    assert "GCP_SCHEDULER_SA_EMAIL" in workflow
    assert "service_timeout: 540s" in workflow
    assert "max_instance_count: 1" in workflow
    assert "max_instance_request_concurrency: 1" in workflow


def test_all_pull_requests_verify_but_only_main_can_deploy():
    workflow = Path(".github/workflows/deploy.yaml").read_text(encoding="utf-8")

    assert "pull_request: {}" in workflow
    assert 'branches: ["main"]' in workflow
    assert "workflow_dispatch:" in workflow
    assert "needs: verify" in workflow
    assert "github.event_name != 'pull_request' && github.ref == 'refs/heads/main'" in workflow


def test_scheduler_update_does_not_delete_the_existing_job():
    workflow = Path(".github/workflows/deploy.yaml").read_text(encoding="utf-8")

    assert "gcloud scheduler jobs update http crypto-rank-tracker-scheduler" in workflow
    assert "gcloud scheduler jobs delete" not in workflow
    assert "--oidc-token-audience=\"$FUNCTION_URL\"" in workflow


def test_scheduler_retries_failed_webhook_executions_with_bounded_backoff():
    workflow = Path(".github/workflows/deploy.yaml").read_text(encoding="utf-8")

    assert "--max-retry-attempts=3" in workflow
    assert "--min-backoff=30s" in workflow
    assert "--max-backoff=5m" in workflow
    assert "--max-doublings=2" in workflow


def test_scheduler_identity_is_authorized_to_invoke_the_gen2_service():
    workflow = Path(".github/workflows/deploy.yaml").read_text(encoding="utf-8")

    assert "gcloud run services add-iam-policy-binding crypto-rank-tracker" in workflow
    assert '--member="serviceAccount:${{ secrets.GCP_SCHEDULER_SA_EMAIL }}"' in workflow
    assert '--role="roles/run.invoker"' in workflow


def test_sector_updater_uses_the_runtime_storage_identity():
    workflow = Path(".github/workflows/updaet-sectors.yaml").read_text(encoding="utf-8")

    assert "GCP_RUNTIME_SA_EMAIL" in workflow
    assert "GCP_SA_EMAIL" not in workflow


def test_deploy_verification_compiles_every_shipped_python_entrypoint():
    workflow = Path(".github/workflows/deploy.yaml").read_text(encoding="utf-8")

    assert "uv run python -m compileall main.py config.py update_sectors.py common tests" in workflow


def test_deploy_verification_imports_every_required_module_from_the_wheel():
    workflow = Path(".github/workflows/deploy.yaml").read_text(encoding="utf-8")

    assert 'WHEEL_PATH="$(realpath dist/*.whl)"' in workflow
    assert "uv run --isolated --with \"$WHEEL_PATH\"" in workflow
    assert "import main, update_sectors, config, common.upbit_client, common.notification.main" in workflow


def test_production_workflows_require_shared_gcs_state():
    deploy = Path(".github/workflows/deploy.yaml").read_text(encoding="utf-8")
    sectors = Path(".github/workflows/updaet-sectors.yaml").read_text(encoding="utf-8")

    assert "STATE_STORAGE_METHOD=GCS" in deploy
    assert "STATE_STORAGE_METHOD: GCS" in sectors
    assert "vars.STATE_STORAGE_METHOD" not in deploy
    assert "vars.STATE_STORAGE_METHOD" not in sectors
    expected_error = "GCS_BUCKET_NAME repository variable is required for durable production state."
    assert expected_error in deploy
    assert expected_error in sectors


def test_production_workflows_select_the_explicit_gcp_project():
    deploy = Path(".github/workflows/deploy.yaml").read_text(encoding="utf-8")
    sectors = Path(".github/workflows/updaet-sectors.yaml").read_text(encoding="utf-8")

    expected_error = "GCP_PROJECT_ID repository secret is required"
    assert expected_error in deploy
    assert expected_error in sectors
    assert "project_id: ${{ secrets.GCP_PROJECT_ID }}" in deploy
    assert "project_id: ${{ secrets.GCP_PROJECT_ID }}" in sectors
    assert "GCP_PROJECT_ID=${{ secrets.GCP_PROJECT_ID }}" in deploy
    assert "--project=\"${{ secrets.GCP_PROJECT_ID }}\"" in deploy


def test_sector_updater_restricts_manual_production_writes_to_main():
    workflow = Path(".github/workflows/updaet-sectors.yaml").read_text(encoding="utf-8")

    assert "github.event_name == 'schedule' || github.ref == 'refs/heads/main'" in workflow


def test_sector_updater_receives_symbol_overrides():
    workflow = Path(".github/workflows/updaet-sectors.yaml").read_text(encoding="utf-8")

    assert "CG_SYMBOL_OVERRIDES: ${{ vars.CG_SYMBOL_OVERRIDES || '{}' }}" in workflow
