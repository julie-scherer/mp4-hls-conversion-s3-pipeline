# COLORS
GREEN  := $(shell tput -Txterm setaf 2)
YELLOW := $(shell tput -Txterm setaf 3)
WHITE  := $(shell tput -Txterm setaf 7)
RESET  := $(shell tput -Txterm sgr0)

TARGET_MAX_CHAR_NUM=20

## Show help with `make help`
help:
	@echo ''
	@echo 'Usage:'
	@echo '  ${YELLOW}make${RESET} ${GREEN}<target>${RESET}'
	@echo ''
	@echo 'Targets:'
	@awk '/^[a-zA-Z\-\_0-9]+:/ { \
		helpMessage = match(lastLine, /^## (.*)/); \
		if (helpMessage) { \
			helpCommand = substr($$1, 0, index($$1, ":")-1); \
			helpMessage = substr(lastLine, RSTART + 3, RLENGTH); \
			helpDir = match(lastLine, /@`([^`]+)`/); \
			if (helpDir) { \
				helpDir = substr(lastLine, RSTART + 2, RLENGTH - 3); \
				printf "  ${YELLOW}%-$(TARGET_MAX_CHAR_NUM)s${RESET} ${GREEN}%s${RESET} in `%s`\n", helpCommand, helpMessage, helpDir; \
			} else { \
				printf "  ${YELLOW}%-$(TARGET_MAX_CHAR_NUM)s${RESET} ${GREEN}%s${RESET}\n", helpCommand, helpMessage; \
			} \
		} \
	} \
	{ lastLine = $$0 }' $(MAKEFILE_LIST)


.PHONY: venv
## Set up virtual environment in current directory (`scripts`)
venv: .venv
	@echo "Virtual environment is ready."

.venv: requirements.txt
	python3 -m venv .venv
	@echo "Virtual environment created."
	@. .venv/bin/activate && pip install --upgrade pip && pip install -r ./requirements.txt
	@touch .venv


.PHONY: upload
## Convert and upload MP4 videos
upload: venv convert_and_upload_videos.py
	@echo "Running video conversion and upload script..."
	@. .venv/bin/activate && python3 convert_and_upload_videos.py
	@echo "Video converted and uploaded successfully."


.PHONY: up
## Run Dockerized app to convert and upload videos
up:
	@docker-compose up --build;

.PHONY: run
run: up

.PHONY: down
## Stop and remove Docker container
down:
	@docker-compose down --remove-orphans --volumes --rmi all; \
	 yes | docker image prune; \
	 yes | docker volume prune -a;

.PHONY: stop
stop: down

