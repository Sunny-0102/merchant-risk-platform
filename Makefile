.PHONY: e2e-local
e2e-local:
	@echo "Running MRP local E2E (raw -> fact -> snapshot)..."
	@bash infra/local/scripts/mrp_e2e.sh

.PHONY: e2e-local-ci
e2e-local-ci:
	@bash infra/local/scripts/e2e_local_ci.sh

.PHONY: dedup-local
dedup-local:
	@echo "Running MRP dedup test (duplicate raw should not duplicate facts)..."
	@bash infra/local/scripts/mrp_dedup.sh

.PHONY: ci-local
ci-local:
	@echo "Running MRP local CI suite (e2e + dedup + idempotency)..."
	@$(MAKE) e2e-local-ci
	@$(MAKE) dedup-local
	@bash infra/local/scripts/mrp_idempotency.sh
