class SyslogClient {
    constructor() {
        this.ws = null;
        this.messages = [];
        this.filteredMessages = [];
        this.autoScroll = true;
        this.startTime = Date.now();

        // Încarcă preferințe din localStorage
        this.showRawMessage = this.loadPreference('showRawMessage', true);
        this.showFilters = this.loadPreference('showFilters', false);
        this.showSearch = false;
        this.searchMode = false;

        // Filtre
        this.filters = {
            search: '',
            hostname: '',
            severity: '',
            facility: '',
            ip: '',
            highlight: ''
        };

        // Paginare
        this.pagination = {
            offset: 0,
            limit: 100,
            total: 0,
            isLoading: false,
            hasMore: true,
            initialLoad: true,
            maxMessages: 1000
        };

        // Fullscreen
        this.fullscreenAutoScroll = true;
        this.fullscreenFilters = {
            search: '',
            source: '',
            severity: '',
            highlight: ''
        };
    }

    // === METODE DE UTILITATE ===
    loadPreference(key, defaultValue) {
        try {
            const saved = localStorage.getItem(key);
            return saved !== null ? JSON.parse(saved) : defaultValue;
        } catch (e) {
            console.warn('Failed to load preference:', key, e);
            return defaultValue;
        }
    }

    savePreference(key, value) {
        try {
            localStorage.setItem(key, JSON.stringify(value));
        } catch (e) {
            console.warn('Failed to save preference:', key, e);
        }
    }

    normalizeMessage(message) {
        const normalized = { ...message };

        normalized.hostname = normalized.hostname || 'unknown';
        normalized.tag = normalized.tag || '';
        normalized.content = normalized.content || 'No content';
        normalized.remote_ip = normalized.remote_ip || 'N/A';
        normalized.raw_message = normalized.raw_message || normalized.content;

        // Parse severity
        if (normalized.severity !== undefined && normalized.severity !== null) {
            if (typeof normalized.severity === 'string') {
                const severityMap = {
                    'emergency': 0, 'alert': 1, 'critical': 2, 'error': 3,
                    'warning': 4, 'notice': 5, 'info': 6, 'debug': 7
                };
                const mappedSeverity = severityMap[normalized.severity.toLowerCase()];
                if (mappedSeverity !== undefined) {
                    normalized.severity = mappedSeverity;
                } else {
                    normalized.severity = 6;
                }
            }
        }
        else if (normalized.priority !== undefined && normalized.priority !== null) {
            normalized.severity = normalized.priority % 8;
        }
        else {
            normalized.severity = 6;
        }

        if (normalized.severity < 0 || normalized.severity > 7) {
            normalized.severity = 6;
        }

        return normalized;
    }

    formatTimestamp(timestamp) {
        try {
            if (!timestamp) return 'No Date';

            const date = new Date(timestamp);
            if (isNaN(date.getTime())) {
                return 'Invalid Date';
            }

            const months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun',
                           'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];
            const month = months[date.getMonth()];
            const day = String(date.getDate()).padStart(2, ' ');
            const hours = String(date.getHours()).padStart(2, '0');
            const minutes = String(date.getMinutes()).padStart(2, '0');
            const seconds = String(date.getSeconds()).padStart(2, '0');

            return `${month} ${day} ${hours}:${minutes}:${seconds}`;
        } catch (e) {
            return 'Invalid Date';
        }
    }

    escapeHtml(unsafe) {
        if (!unsafe) return '';
        return unsafe
            .replace(/&/g, "&amp;")
            .replace(/</g, "&lt;")
            .replace(/>/g, "&gt;")
            .replace(/"/g, "&quot;")
            .replace(/'/g, "&#039;");
    }

    applyHighlight(text, highlightTerm) {
        if (!highlightTerm || !text) return this.escapeHtml(text);

        const escapedText = this.escapeHtml(text);
        const escapedHighlight = this.escapeHtml(highlightTerm);

        if (!escapedHighlight) return escapedText;

        const regex = new RegExp(`(${escapedHighlight.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')})`, 'gi');
        return escapedText.replace(regex, '<span class="highlight">$1</span>');
    }

    getSeverityString(severityCode) {
        const severities = ['emergency', 'alert', 'critical', 'error', 'warning', 'notice', 'info', 'debug'];
        return severities[severityCode] || 'unknown';
    }

    getFacilityString(facilityCode) {
        const facilities = [
            'kern', 'user', 'mail', 'daemon', 'auth', 'syslog', 'lpr', 'news',
            'uucp', 'cron', 'authpriv', 'ftp', 'ntp', 'logaudit', 'logalert', 'clock',
            'local0', 'local1', 'local2', 'local3', 'local4', 'local5', 'local6', 'local7'
        ];
        return facilities[facilityCode] || 'unknown';
    }

    // === METODE DE BAZĂ ===
    scrollToTop() {
        const container = document.getElementById('messagesContainer');
        if (container) {
            container.scrollTop = 0;
        }
    }

    updateStats() {
        const totalEl = document.getElementById('totalMessages');
        const loadedEl = document.getElementById('loadedMessages');

        if (totalEl) totalEl.textContent = this.pagination.total.toLocaleString();
        if (loadedEl) loadedEl.textContent = this.messages.length.toLocaleString();
    }

	updateTimestamps() {
	    const now = new Date();
	    const hours = String(now.getHours()).padStart(2, '0');
	    const minutes = String(now.getMinutes()).padStart(2, '0');
	    const seconds = String(now.getSeconds()).padStart(2, '0');
	    const timestamp = `${hours}:${minutes}:${seconds}`;

	    // Update în view-ul principal
	    const mainUpdateElement = document.getElementById('lastUpdate');
	    if (mainUpdateElement) {
	        mainUpdateElement.textContent = timestamp;
	    }

	    // Update în fullscreen (dacă elementul există)
	    const fullscreenUpdateElement = document.getElementById('fullscreenLastUpdate');
	    if (fullscreenUpdateElement) {
	        fullscreenUpdateElement.textContent = timestamp;
	    }

	    return timestamp;
	}

    async loadMessageCount() {
        try {
            const response = await fetch('api/messages/count');
            if (response.ok) {
                const data = await response.json();
                this.pagination.total = data.total;
                this.updateStats();
            }
        } catch (error) {
            this.pagination.total = 0;
        }
    }

    async loadMoreMessages() {
        // Dacă suntem în search mode, folosește search pagination
        if (this.searchMode && this.searchManager) {
            return this.searchManager.performSearch(true);
        }

        if (this.pagination.isLoading || !this.pagination.hasMore) {
            return;
        }

        // Limită de mesaje în memorie
        if (this.messages.length >= this.pagination.maxMessages) {
            console.warn(`Reached maximum message limit (${this.pagination.maxMessages})`);
            this.pagination.hasMore = false;
            this.uiManager?.updateLoadMoreButton?.();
            return;
        }

        this.pagination.isLoading = true;
        this.uiManager?.updateLoadMoreButton?.();

        try {
            const response = await fetch(`api/messages/paginated?offset=${this.pagination.offset}&limit=${this.pagination.limit}`);

            if (response.ok) {
                const data = await response.json();
                const newMessages = data.messages || [];
                const paginationInfo = data.pagination || {};

                const normalizedMessages = newMessages.map(msg => this.normalizeMessage(msg));

                this.messages = [...this.messages, ...normalizedMessages];
                this.pagination.total = paginationInfo.total || this.pagination.total;

                // Verifică limita
                const remainingSlots = this.pagination.maxMessages - this.messages.length;
                this.pagination.hasMore = (this.pagination.offset + newMessages.length) < this.pagination.total && remainingSlots > 0;
                this.pagination.offset += newMessages.length;

                this.filterManager?.applyFilters?.();
                this.updateStats();
                this.updateTimestamps();

                if (this.pagination.initialLoad) {
                    this.pagination.initialLoad = false;
                    setTimeout(() => this.scrollToTop(), 100);
                }
            } else {
                this.pagination.hasMore = false;
            }
        } catch (error) {
            this.pagination.hasMore = false;
        } finally {
            this.pagination.isLoading = false;
            this.uiManager?.updateLoadMoreButton?.();
        }
    }

    clearMessages() {
        this.messages = [];
        this.filteredMessages = [];
        this.pagination.offset = 0;
        this.pagination.total = 0;
        this.pagination.hasMore = true;
        this.pagination.initialLoad = true;

        this.uiManager?.renderMessages?.();
        this.updateStats();
        this.updateTimestamps();
        this.loadMessageCount();
        this.uiManager?.updateLoadMoreButton?.();
    }

    toggleAutoScroll() {
        this.autoScroll = !this.autoScroll;
        const statusElement = document.getElementById('autoScrollStatus');
        if (statusElement) {
            statusElement.textContent = this.autoScroll ? 'On' : 'Off';
        }

        if (this.autoScroll) {
            this.scrollToTop();
        }
    }

	exportMessages() {
	    // Determină ce mesaje să exporte
	    let messagesToExport;

	    if (this.searchMode && this.messages.length > 0) {
	        // În modul search, exportă mesajele din search
	        messagesToExport = this.messages;
	    } else if (this.filteredMessages.length > 0) {
	        // Altfel, exportă mesajele filtrate
	        messagesToExport = this.filteredMessages;
	    } else {
	        // Fallback la toate mesajele
	        messagesToExport = this.messages;
	    }

	    // Creează obiectul pentru export cu metadate
	    const exportData = {
	        meta: {
	            exported_at: new Date().toISOString(),
	            mode: this.searchMode ? 'search_results' : 'live_messages',
	            total_in_buffer: this.messages.length,
	            filtered_count: this.filteredMessages.length,
	            exported_count: messagesToExport.length,
	            search_mode: this.searchMode,
	            search_criteria: this.searchMode && this.searchManager?.currentSearchParams
	                ? this.searchManager.currentSearchParams
	                : null
	        },
	        messages: messagesToExport.map(msg => ({
	            id: msg.id,
	            timestamp: msg.timestamp,
	            hostname: msg.hostname,
	            tag: msg.tag,
	            content: msg.content,
	            severity: msg.severity,
	            severity_string: this.getSeverityString(msg.severity),
	            facility: msg.facility,
	            facility_string: this.getFacilityString(msg.facility),
	            remote_ip: msg.remote_ip,
	            protocol: msg.protocol,
	            raw_message: msg.raw_message,
	            created_at: msg.created_at
	        }))
	    };

	    const dataStr = JSON.stringify(exportData, null, 2);
	    const dataBlob = new Blob([dataStr], {type: 'application/json'});

	    const link = document.createElement('a');
	    link.href = URL.createObjectURL(dataBlob);

	    // Nume de fișier diferit pentru search vs normal
	    const dateStr = new Date().toISOString().split('T')[0];
	    const mode = this.searchMode ? 'search' : 'live';
	    link.download = `syslog-export-${mode}-${dateStr}.json`;

	    link.click();
	    URL.revokeObjectURL(link.href);

	    // Feedback visual
	    this.showExportToast(messagesToExport.length, this.searchMode);
	}

	showExportToast(count, isSearch) {
	    const toast = document.createElement('div');
	    toast.style.cssText = `
	        position: fixed;
	        bottom: 20px;
	        right: 20px;
	        background: #28a745;
	        color: white;
	        padding: 12px 20px;
	        border-radius: 4px;
	        z-index: 10000;
	        font-size: 14px;
	        box-shadow: 0 4px 12px rgba(0,0,0,0.15);
	    `;
	    toast.textContent = `✅ Exported ${count} ${isSearch ? 'search results' : 'messages'} to JSON`;
	    document.body.appendChild(toast);

	    setTimeout(() => {
	        toast.style.opacity = '0';
	        toast.style.transition = 'opacity 0.3s';
	        setTimeout(() => toast.remove(), 300);
	    }, 3000);
	}

    // === METODE DELEGATE === (pentru manageri)
    applyFilters() {
        this.filterManager?.applyFilters?.();
    }

    renderMessages() {
        this.uiManager?.renderMessages?.();
    }

    updateFullscreenDisplay() {
        this.fullscreenManager?.updateFullscreenDisplay?.();
    }

    updateLoadMoreButton() {
        this.uiManager?.updateLoadMoreButton?.();
    }

    toggleFilters() {
        this.uiManager?.toggleFilters?.();
    }

    toggleSearch() {
        this.uiManager?.toggleSearch?.();
    }
}