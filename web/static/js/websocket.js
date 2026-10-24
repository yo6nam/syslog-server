class WebSocketManager {
    constructor(client) {
        this.client = client;
        this.ws = null;
        this.reconnectAttempts = 0;
        this.pendingQueue = [];
        this.flushScheduled = false;
        this.FLUSH_DELAY_MS = 200;
    }

    static basePath(pathname = window.location.pathname) {
        return pathname.replace(/[^/]*$/, '');
    }

    connect() {
        const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
        const wsUrl = `${protocol}//${window.location.host}${WebSocketManager.basePath()}ws`;

        this.ws = new WebSocket(wsUrl);

        this.ws.onopen = () => {
            this.updateConnectionStatus(true);
            this.reconnectAttempts = 0;
        };

        this.ws.onmessage = (event) => {
            this.queueMessage(JSON.parse(event.data));
        };

        this.ws.onclose = () => {
            this.updateConnectionStatus(false);
            const delay = Math.min(1000 * Math.pow(2, this.reconnectAttempts), 30000);
            setTimeout(() => this.connect(), delay);
            this.reconnectAttempts++;
        };

        this.ws.onerror = (error) => {
            console.error('WebSocket error:', error);
        };
    }

    queueMessage(message) {
        if (this.client.searchMode) {
            return;
        }

        this.pendingQueue.push(message);

        if (!this.flushScheduled) {
            this.flushScheduled = true;
            setTimeout(() => this.flushQueue(), this.FLUSH_DELAY_MS);
        }
    }

    flushQueue() {
        this.flushScheduled = false;
        if (this.pendingQueue.length === 0) return;

        const batch = this.pendingQueue;
        this.pendingQueue = [];

        const normalizedBatch = batch.map(message => {
            if (!message._receivedAt) {
                message._receivedAt = new Date().toISOString();
            }
            return this.client.normalizeMessage(message);
        });

        const max = this.client.pagination.maxMessages;

        this.client.messages = [...normalizedBatch.slice().reverse(), ...this.client.messages];
        this.client.pagination.total += normalizedBatch.length;
        if (this.client.messages.length > max) {
            this.client.messages = this.client.messages.slice(0, max);
        }

        const filterManager = this.client.filterManager;
        const newlyVisible = filterManager
            ? normalizedBatch.filter(msg => filterManager.matchesFilters(msg))
            : normalizedBatch;

        if (newlyVisible.length > 0) {
            this.client.filteredMessages = [...newlyVisible.slice().reverse(), ...this.client.filteredMessages];
            if (this.client.filteredMessages.length > max) {
                this.client.filteredMessages = this.client.filteredMessages.slice(0, max);
            }
        }

        this.client.updateStats();
        this.client.updateTimestamps?.();

        if (this.client.autoScroll) {
            if (newlyVisible.length > 0) {
                this.client.uiManager?.prependMessages?.(newlyVisible.slice().reverse());
            }
        } else {
            this.client.pendingNewCount = (this.client.pendingNewCount || 0) + newlyVisible.length;
            this.client.uiManager?.updateNewMessagesIndicator?.();
        }

        const fullscreenLogs = document.getElementById('fullscreenLogs');
        if (fullscreenLogs && !fullscreenLogs.classList.contains('hidden')) {
            this.client.fullscreenManager?.updateFullscreenDisplay?.();
        }
    }

    updateConnectionStatus(connected) {
        const statusElement = document.getElementById('connectionStatus');
        const statusText = document.getElementById('statusText');

        if (connected) {
            statusElement.className = 'connection-status connected';
            statusText.textContent = 'CONNECTED';
        } else {
            statusElement.className = 'connection-status disconnected';
            statusText.textContent = 'DISCONNECTED';
        }
    }
}
