include .env.local
define RUN_WITH_ENV
/bin/sh -lc 'set -euo pipefail; set -a; . $(1); set +a; $(2)'
endef

K8S_NAMESPACE ?= nats-s3-monitor
CHART_INSTANCE ?= my-nats-s3-monitor

start_api:
	$(call RUN_WITH_ENV, .env.local, start_api -f secrets/config.yaml)

up:
	docker compose --env-file .env.local -f compose-nats-s3-monitor.yaml -p ${TEAM_NAME} up -d

down:
	docker compose --env-file .env.local -f compose-nats-s3-monitor.yaml -p ${TEAM_NAME} down || echo "No running containers"

shell:
	docker run -it --env-file .env.local --rm --entrypoint /bin/bash ${DOCKER_IMAGE}:${DOCKER_TAG}

build:
	docker build \
		-f docker/Dockerfile \
		-t ${DOCKER_IMAGE}:${DOCKER_TAG} \
		.

local-creds:
	@mkdir -p secrets
	nats context select ${NATS_SYSTEM_CONTEXT}
	$(call RUN_WITH_ENV, .env.local, curl -fsSL https://raw.githubusercontent.com/jr200/nats-infra/main/scripts/nats-create-account.sh | /bin/bash -s -- > secrets/sa-nats-s3-monitor.creds)


.PHONY: check
check:
	ruff check --fix
	ruff format
	mypy .

chart-deps:
	kubectl create namespace ${K8S_NAMESPACE} || echo "OK"
	
	kubectl create secret generic -n ${K8S_NAMESPACE} nats-s3-monitor-env \
	--from-env-file=.env.local.k8s || echo "OK"

	kubectl create secret generic -n ${K8S_NAMESPACE} nats-user-credentials \
	--from-file=app.creds=secrets/sa-nats-s3-monitor.creds || echo "OK"

	kubectl create configmap -n ${K8S_NAMESPACE} nats-s3-monitor-config \
	--from-file=config.yaml=secrets/config.yaml || echo "OK"


chart-install: chart-deps
	kubectl create namespace ${K8S_NAMESPACE} || echo "OK"
	helm upgrade --install -n ${K8S_NAMESPACE} ${CHART_INSTANCE} -f charts/values.yaml stakater/application

chart-template:
	helm template --debug -n ${K8S_NAMESPACE} ${CHART_INSTANCE} -f charts/values.yaml stakater/application > charts/zz_rendered.yaml

chart-uninstall:
	helm uninstall -n ${K8S_NAMESPACE} ${CHART_INSTANCE} || echo "OK"
	kubectl delete secret -n ${K8S_NAMESPACE} nats-s3-monitor-env || echo "OK"
	kubectl delete secret -n ${K8S_NAMESPACE} nats-user-credentials || echo "OK"
	kubectl delete configmap -n ${K8S_NAMESPACE} nats-s3-monitor-config || echo "OK"

docker-login:
	$(call RUN_WITH_ENV, .env.local, docker login -u $${DOCKER_USERNAME} -p $${DOCKER_PASSWORD} $${DOCKER_SERVER})
