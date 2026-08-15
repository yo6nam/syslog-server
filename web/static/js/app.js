class App {
    constructor() {
        this.client = new SyslogClient();
        this.wsManager = new WebSocketManager(this.client);
        this.uiManager = new UIManager(this.client);
        this.filterManager = new FilterManager(this.client);
        this.searchManager = new SearchManager(this.client);
        this.fullscreenManager = new FullscreenManager(this.client);

        // Conectează managerii la client
        this.client.wsManager = this.wsManager;
        this.client.uiManager = this.uiManager;
        this.client.filterManager = this.filterManager;
        this.client.searchManager = this.searchManager;
        this.client.fullscreenManager = this.fullscreenManager;
    }

    async init() {
        await this.client.loadMessageCount();
        await this.client.loadMoreMessages();
        this.wsManager.connect();
        this.uiManager.startUptimeCounter();
        this.setupEventListeners();
        this.setupInfiniteScroll();
        this.fullscreenManager.setupFullscreenEventListeners();
        this.uiManager.restoreCheckboxState();
        this.uiManager.restoreFiltersState();
        this.client.updateTimestamps?.();
    }

    setupEventListeners() {
        let searchTimeout;

        // Search filter
        const searchFilter = document.getElementById('searchFilter');
        if (searchFilter) {
            searchFilter.addEventListener('input', (e) => {
                clearTimeout(searchTimeout);
                searchTimeout = setTimeout(() => {
                    this.client.filters.search = e.target.value;
                    this.filterManager.applyFilters();
                }, 300);
            });
        }

        // Hostname filter
        const hostnameFilter = document.getElementById('hostnameFilter');
        if (hostnameFilter) {
            hostnameFilter.addEventListener('input', (e) => {
                clearTimeout(searchTimeout);
                searchTimeout = setTimeout(() => {
                    this.client.filters.hostname = e.target.value;
                    this.filterManager.applyFilters();
                }, 300);
            });
        }

        // Highlight filter
        const highlightFilter = document.getElementById('highlightFilter');
        if (highlightFilter) {
            highlightFilter.addEventListener('input', (e) => {
                clearTimeout(searchTimeout);
                searchTimeout = setTimeout(() => {
                    this.client.filters.highlight = e.target.value;
                    this.filterManager.applyFilters();
                }, 300);
            });
        }

        // Severity filter
        const severityFilter = document.getElementById('severityFilter');
        if (severityFilter) {
            severityFilter.addEventListener('change', (e) => {
                this.client.filters.severity = e.target.value;
                this.filterManager.applyFilters();
            });
        }

        // Raw Messages checkbox
        const showRawMessage = document.getElementById('showRawMessage');
        if (showRawMessage) {
            showRawMessage.addEventListener('change', (e) => {
                this.client.showRawMessage = e.target.checked;
                this.client.savePreference('showRawMessage', this.client.showRawMessage);
                this.uiManager.renderMessages();
            });
        }

        // Auto-scroll tracking
        const messagesContainer = document.getElementById('messagesContainer');
        if (messagesContainer) {
            messagesContainer.addEventListener('scroll', () => {
                const isAtTop = messagesContainer.scrollTop <= 10;

                if (!isAtTop && this.client.autoScroll) {
                    this.client.autoScroll = false;
                    const statusElement = document.getElementById('autoScrollStatus');
                    if (statusElement) {
                        statusElement.textContent = 'Off';
                    }
                } else if (isAtTop && !this.client.autoScroll) {
                    this.client.autoScroll = true;
                    const statusElement = document.getElementById('autoScrollStatus');
                    if (statusElement) {
                        statusElement.textContent = 'On';
                    }
                    this.client.resumeLive();
                }
            });
        }
    }

    setupInfiniteScroll() {
        const messagesContainer = document.getElementById('messagesContainer');
        if (messagesContainer) {
            messagesContainer.addEventListener('scroll', () => {
                if (this.client.pagination.isLoading || !this.client.pagination.hasMore) {
                    return;
                }

                const scrollThreshold = 100;
                const isNearBottom = messagesContainer.scrollHeight - messagesContainer.clientHeight <=
                    messagesContainer.scrollTop + scrollThreshold;

                if (isNearBottom) {
                    this.client.loadMoreMessages();
                }
            });
        }
    }
}

// === FUNCȚII GLOBALE ===
function clearMessages() {
    if (app && app.client && confirm('Are you sure you want to clear all messages? This cannot be undone.')) {
        app.client.clearMessages();
    }
}

function toggleAutoScroll() {
    if (app && app.client) {
        app.client.toggleAutoScroll();
    }
}

function jumpToLatest() {
    if (app && app.client) {
        app.client.jumpToLatest();
    }
}

function exportMessages() {
    if (app && app.client) {
        app.client.exportMessages();
    }
}

function loadMoreMessages() {
    if (app && app.client) {
        app.client.loadMoreMessages();
    }
}

function clearFilters() {
    if (app && app.filterManager) {
        app.filterManager.clearFilters();
    }
}

function toggleFilters() {
    if (app && app.uiManager) {
        app.uiManager.toggleFilters();
    }
}

function toggleSearch() {
    if (app && app.uiManager) {
        app.uiManager.toggleSearch();
    }
}

function performSearch() {
    if (app && app.searchManager) {
        app.searchManager.performSearch();
    }
}

function searchLast24Hours() {
    if (app && app.searchManager) {
        app.searchManager.searchLast24Hours();
    }
}

function clearSearch() {
    if (app && app.searchManager) {
        app.searchManager.clearSearch();
    }
}

function toggleFullscreenLogs() {
    if (app && app.fullscreenManager) {
        app.fullscreenManager.toggleFullscreenLogs();
    }
}

function toggleFullscreenAutoScroll() {
    if (app && app.fullscreenManager) {
        app.fullscreenManager.toggleFullscreenAutoScroll();
    }
}

// Inițializare aplicație
let app;
document.addEventListener('DOMContentLoaded', () => {
    app = new App();
    app.init();
});