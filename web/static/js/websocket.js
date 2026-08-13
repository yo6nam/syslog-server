class WebSocketManager {
    constructor(client) {
        this.client = client;
        this.ws = null;
        this.reconnectAttempts = 0;
    }

    connect() {
        const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
        const wsUrl = `${protocol}//${window.location.host}/ws`;

        this.ws = new WebSocket(wsUrl);

        this.ws.onopen = () => {
            this.updateConnectionStatus(true);
            this.reconnectAttempts = 0;
        };

        this.ws.onmessage = (event) => {
            this.handleMessage(JSON.parse(event.data));
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

    handleMessage(message) {
        if (this.client.searchMode) {
            return;
        }

        if (!message._receivedAt) {
            message._receivedAt = new Date().toISOString();
        }

        const normalizedMessage = this.client.normalizeMessage(message);

        this.client.messages.unshift(normalizedMessage);
        this.client.pagination.total++;

        if (this.client.messages.length > this.client.pagination.maxMessages) {
            this.client.messages = this.client.messages.slice(0, this.client.pagination.maxMessages);
        }

        this.client.filterManager?.applyFilters?.();
        this.client.updateStats();
        this.client.updateTimestamps?.();

        if (!document.getElementById('fullscreenLogs').classList.contains('hidden')) {
            this.client.fullscreenManager?.updateFullscreenDisplay?.();
        }

        if (this.client.autoScroll) {
            setTimeout(() => this.client.scrollToTop(), 50);
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