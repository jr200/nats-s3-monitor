include .env.local

K8S_NAMESPACE ?= nats-s3-monitor
CHART_NAME ?= bento-custom-chart

up:
	docker compose --env-file .env.local -f compose-nats-s3-monitor.yaml -p ${TEAM_NAME} up -d

down:
	docker compose  -f compose-nats-s3-monitor.yaml -p ${TEAM_NAME} down || echo "No running containers"

.PHONY: shell
shell:
	docker run -it -v ./charts/bento/files/scripts:/scripts --user root --env-file .env.local --rm --entrypoint /bin/bash ${BENTO_IMAGE}

build:
	docker build \
		-f docker/Dockerfile \
		-t ${DOCKER_IMAGE}:${DOCKER_TAG} \
		.

nats-local-creds:
	@mkdir -p secrets
	nats context select ${NATS_SYSTEM_CONTEXT}
	/bin/bash -lc 'set -euo pipefail; set -a; source .env.local; set +a; curl -fsSL https://raw.githubusercontent.com/jr200/nats-infra/main/scripts/nats-create-account.sh | /bin/bash -s -- > secrets/sa-nats-s3-monitor.creds'

nats-create-stream:
	nats stream add ${OUTPUT_NATS_STREAM} \
	--subjects ${OUTPUT_NATS_SUBJECT} \
	--storage file \
	--replicas=1 \
	--retention=work \
	--discard=old \
	--max-msgs=-1 \
	--max-msgs-per-subject=-1 \
	--max-bytes=-1 \
	--max-age=1M \
	--max-msg-size=1k \
	--dupe-window=1d \
	--no-allow-rollup \
	--no-deny-delete \
	--no-deny-purge

.PHONY: check
check:
	ruff check --fix
	ruff format
	mypy .

chart-secrets:
	kubectl create namespace ${K8S_NAMESPACE} || echo "OK"
	source .env.local.k8s && \
		kubectl create secret generic -n ${K8S_NAMESPACE} ${CHART_NAME}-bento-secrets \
		--from-env-file=.env.local.k8s|| echo "OK"

chart-install: chart-secrets
	helm upgrade --install -n ${K8S_NAMESPACE} ${CHART_NAME} charts/bento

chart-template:
	helm template --debug -n ${K8S_NAMESPACE} ${CHART_NAME} charts/bento