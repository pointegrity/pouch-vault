.DEFAULT_GOAL := help

BINARY := pouch-vault

# --- Deploy (remote: rtx1059 default; future hosts via override) ---
#
# One-time setup (rtx, as user jy):
#   ssh rtx1059
#   mkdir -p ~/infra/pouch-vault/data
#   cd ~/infra/pouch-vault
#   git clone git@github.com:pointegrity/pouch-vault.git
#   cd pouch-vault
#   make deploy-build           # builds ./build/pouch-vault
#   sudo cp deploy/pouch-vault.service /etc/systemd/system/
#   sudo install -m 0600 -o root -g jy deploy/pouch-vault.env.example /etc/default/pouch-vault
#   $EDITOR /etc/default/pouch-vault       # fill in vault key + hmac
#   sudo systemctl daemon-reload
#   sudo systemctl enable --now pouch-vault
#
# Day-to-day (from local repo, runs on remote via ssh):
#   make deploy             # pull + build + restart
#
DEPLOY_HOST  ?= rtx1059
DEPLOY_USER  ?= jy
DEPLOY_WS    ?= /home/jy/infra/pouch-vault
DEPLOY_SVC   ?= pouch-vault
DEPLOY_SSH   := ssh $(DEPLOY_HOST)
DEPLOY_BASH  ?= bash -lc
DEPLOY_GOTOOLCHAIN ?= go1.25.5

help: ## Show commands
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?##/ {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

build: ## Build the pouch-vault binary locally
	go build -o ./build/$(BINARY) .

deploy-pull: ## Git pull pouch-vault on remote
	$(DEPLOY_SSH) "$(DEPLOY_BASH) 'cd $(DEPLOY_WS)/pouch-vault && git pull --ff-only'"

deploy-build: ## Rebuild pouch-vault binary on remote
	$(DEPLOY_SSH) "$(DEPLOY_BASH) 'export PATH=/usr/local/go/bin:\$$PATH && cd $(DEPLOY_WS)/pouch-vault && GOTOOLCHAIN=$(DEPLOY_GOTOOLCHAIN) CGO_ENABLED=1 go build -o ./build/$(BINARY) . && ls -l ./build/$(BINARY)'"

deploy-restart: ## Restart the pouch-vault systemd service on remote
	$(DEPLOY_SSH) "sudo systemctl restart $(DEPLOY_SVC) && systemctl is-active $(DEPLOY_SVC)"

deploy: deploy-pull deploy-build deploy-restart ## Pull + build + restart

deploy-status: ## Show service status on remote
	$(DEPLOY_SSH) "systemctl status $(DEPLOY_SVC) --no-pager"

deploy-logs: ## Tail remote journal (Ctrl-C to exit)
	$(DEPLOY_SSH) "journalctl -u $(DEPLOY_SVC) -f --no-pager"

clean: ## Remove built artifacts
	rm -rf build/

.PHONY: help build deploy deploy-pull deploy-build deploy-restart deploy-status deploy-logs clean
