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
	GOWORK=off go build -o ./build/$(BINARY) .

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

# --- Local (macOS) launchd: run pouch-vault as a long-lived agent ---
#
# Survives reboot + login (RunAtLoad), auto-restarts on crash
# (KeepAlive). Binary + config under ~/Library/Application Support/
# pouch-vault/. Plist at ~/Library/LaunchAgents/.
#
# First install:
#   make local-install        # creates env stub; then:
#   $EDITOR "$$(make -s local-config-path)"
#   make local-restart        # picks up the edited env
#
# Daily verbs:
#   make local-status         # is it running? + last log lines + drop count
#   make local-logs           # tail -F both log streams
#   make local-ui             # open http://127.0.0.1:.../ui in browser
#   make local-restart        # rebuild + bounce
#   make local-uninstall      # unload + delete plist (keeps data)
LOCAL_PREFIX  ?= $(HOME)/Library/Application Support/pouch-vault
LOCAL_BINARY  := $(LOCAL_PREFIX)/pouch-vault
LOCAL_CONFIG  := $(LOCAL_PREFIX)/vault.env
LOCAL_PLIST   := $(HOME)/Library/LaunchAgents/com.pointegrity.pouch-vault.plist
LOCAL_LOGDIR  := $(HOME)/Library/Logs
LOCAL_LOGOUT  := $(LOCAL_LOGDIR)/pouch-vault.out.log
LOCAL_LOGERR  := $(LOCAL_LOGDIR)/pouch-vault.err.log
LOCAL_LABEL   := com.pointegrity.pouch-vault

local-install: build  ## Install + load the launchd agent (first-time setup)
	@mkdir -p "$(LOCAL_PREFIX)/data/blobs" "$(LOCAL_LOGDIR)"
	@cp ./build/$(BINARY) "$(LOCAL_BINARY)"
	@if [ ! -f "$(LOCAL_CONFIG)" ]; then \
		sed -e 's|__PREFIX__|$(LOCAL_PREFIX)|g' -e 's|__HOME__|$(HOME)|g' \
		    deploy/pouch-vault.macos.env.example > "$(LOCAL_CONFIG)"; \
		chmod 600 "$(LOCAL_CONFIG)"; \
		echo ">> $(LOCAL_CONFIG) created from template. Edit it (fill POUCH_VAULT_KEY/HMAC), then 'make local-restart'."; \
	fi
	@sed -e 's|__BINARY__|$(LOCAL_BINARY)|g' \
	     -e 's|__CONFIG__|$(LOCAL_CONFIG)|g' \
	     -e 's|__LOGOUT__|$(LOCAL_LOGOUT)|g' \
	     -e 's|__LOGERR__|$(LOCAL_LOGERR)|g' \
	     -e 's|__WORKDIR__|$(LOCAL_PREFIX)|g' \
	     deploy/com.pointegrity.pouch-vault.plist > "$(LOCAL_PLIST)"
	@launchctl bootout gui/$$(id -u)/$(LOCAL_LABEL) 2>/dev/null || true
	@launchctl bootstrap gui/$$(id -u) "$(LOCAL_PLIST)"
	@echo ">> Installed. 'make local-status' to verify."

local-uninstall:  ## Stop + remove the agent (keeps $(LOCAL_PREFIX) data)
	@launchctl bootout gui/$$(id -u)/$(LOCAL_LABEL) 2>/dev/null || true
	@rm -f "$(LOCAL_PLIST)"
	@echo ">> Uninstalled. Data + config at $(LOCAL_PREFIX) preserved."

local-restart: build  ## Rebuild binary + bounce the agent
	@cp ./build/$(BINARY) "$(LOCAL_BINARY)"
	@launchctl kickstart -k gui/$$(id -u)/$(LOCAL_LABEL) 2>/dev/null \
		|| (echo ">> Agent not installed; running 'make local-install'."; $(MAKE) local-install)
	@sleep 1 && $(MAKE) local-status

local-status:  ## Show launchd state + last log lines + drop count
	@echo "=== launchd ($(LOCAL_LABEL)) ==="
	@launchctl list | awk -v lbl="$(LOCAL_LABEL)" 'NR==1 || $$3==lbl' || echo "(not loaded)"
	@echo
	@echo "=== last stdout (10 lines) ==="
	@tail -n 10 "$(LOCAL_LOGOUT)" 2>/dev/null || echo "(no stdout yet)"
	@echo
	@echo "=== last stderr (10 lines) ==="
	@tail -n 10 "$(LOCAL_LOGERR)" 2>/dev/null || echo "(no stderr yet)"
	@echo
	@echo "=== local UI ==="
	@LISTEN=$$(grep '^VAULT_LISTEN=' "$(LOCAL_CONFIG)" 2>/dev/null | cut -d= -f2-); \
	 if [ -n "$$LISTEN" ]; then echo "http://$$LISTEN/ui"; else echo "(VAULT_LISTEN not set in $(LOCAL_CONFIG))"; fi
	@echo "=== drops db (consumer state) ==="
	@DB=$$(grep '^VAULT_DB=' "$(LOCAL_CONFIG)" 2>/dev/null | cut -d= -f2-); \
	 if [ -f "$$DB" ]; then sqlite3 "$$DB" "SELECT COUNT(*) || ' drop(s); newest: ' || COALESCE(MAX(received_at),'(none)') FROM drops;"; else echo "(no incoming drops yet — fine for producer-only vaults)"; fi
	@echo "=== sync.json (producer state) ==="
	@SYNC="$(LOCAL_PREFIX)/sync.json"; \
	 if [ -f "$$SYNC" ]; then \
	   COUNT=$$(python3 -c "import json,sys; d=json.load(open('$$SYNC')); n=sum(len(v.get('files',{})) for v in d.get('paths',{}).values()); print(n)" 2>/dev/null || echo "?"); \
	   echo "$$COUNT file(s) tracked across watched paths"; \
	 else echo "(no producer state — no direction=watch paths declared, or no scan yet)"; fi

local-logs:  ## Tail both log streams (Ctrl-C to exit)
	@touch "$(LOCAL_LOGOUT)" "$(LOCAL_LOGERR)"
	tail -F "$(LOCAL_LOGOUT)" "$(LOCAL_LOGERR)"

local-ui:  ## Open the vault's local UI in default browser
	@LISTEN=$$(grep '^VAULT_LISTEN=' "$(LOCAL_CONFIG)" 2>/dev/null | cut -d= -f2-); \
	 if [ -z "$$LISTEN" ]; then echo "VAULT_LISTEN not set in $(LOCAL_CONFIG)"; exit 1; fi; \
	 URL="http://$$LISTEN/ui"; \
	 echo "Opening $$URL"; \
	 open "$$URL"

local-config-path:  ## Print the env file path (for $$EDITOR / scripting)
	@echo "$(LOCAL_CONFIG)"

.PHONY: help build deploy deploy-pull deploy-build deploy-restart deploy-status deploy-logs clean \
        local-install local-uninstall local-restart local-status local-logs local-ui local-config-path
