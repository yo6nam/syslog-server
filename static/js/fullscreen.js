class FullscreenManager {
    constructor(client) {
        this.client = client;
    }

	toggleFullscreenLogs() {
	    const container = document.getElementById('fullscreenLogs');
	    if (container) {
	        container.classList.toggle('hidden');

	        if (!container.classList.contains('hidden')) {
	            // Sincronizează highlight-ul dacă e gol în fullscreen dar există în principale
	            if (!this.client.fullscreenFilters.highlight && this.client.filters.highlight) {
	                const fullscreenHighlightInput = document.getElementById('fullscreenHighlight');
	                if (fullscreenHighlightInput) {
	                    fullscreenHighlightInput.value = this.client.filters.highlight;
	                    this.client.fullscreenFilters.highlight = this.client.filters.highlight;
	                }
	            }
	            this.updateFullscreenDisplay();
	        }
	    }
	}

	updateFullscreenDisplay() {
	    const container = document.getElementById('fullscreenMessagesList');
	    if (!container) return;

	    const filtered = this.applyFullscreenFilters();
	    const reversedMessages = [...filtered].reverse();

	    container.innerHTML = reversedMessages.map(msg => {
	        const timestamp = this.client.formatTimestamp(msg.timestamp);
	        const severity = this.client.getSeverityString(msg.severity);
	        const severityDisplay = severity.toUpperCase();
	        const source = msg.hostname || msg.remote_ip || 'unknown';
	        const facility = this.client.getFacilityString(msg.facility).toUpperCase();

	        // CORECTAT: Folosește fullscreenFilters.highlight în loc de filters.highlight
	        const content = this.client.applyHighlight(
	            this.client.escapeHtml(msg.content || ''),
	            this.client.fullscreenFilters.highlight // Schimbat aici
	        );

	        return `
	        <div class="fullscreen-message">
	            <span class="fullscreen-timestamp">${timestamp}</span>
	            <span class="fullscreen-severity ${severity}">${severityDisplay}</span>
	            <span class="fullscreen-source">${source}</span>
	            <span class="fullscreen-facility">${facility}</span>
	            <span class="fullscreen-content">${content}</span>
	        </div>`;
	    }).join('');

	    this.updateFullscreenStats(filtered.length);
	}

    applyFullscreenFilters() {
        const search = this.client.fullscreenFilters.search.toLowerCase();
        const source = this.client.fullscreenFilters.source.toLowerCase();
        const severity = this.client.fullscreenFilters.severity;

        return this.client.messages.filter(msg => {
            const matchesSearch = !search ||
                (msg.content && msg.content.toLowerCase().includes(search)) ||
                (msg.raw_message && msg.raw_message.toLowerCase().includes(search));

            const msgSource = (msg.hostname || msg.remote_ip || '').toLowerCase();
            const matchesSource = !source || msgSource.includes(source);

            const severityMap = {
                'emergency': 0, 'alert': 1, 'critical': 2, 'error': 3,
                'warning': 4, 'notice': 5, 'info': 6, 'debug': 7
            };
            const severityNumber = severityMap[severity];
            const matchesSeverity = !severity || msg.severity === severityNumber;

            return matchesSearch && matchesSource && matchesSeverity;
        });
    }

    updateFullscreenStats(shownCount) {
        const fullscreenTotal = document.getElementById('fullscreenTotal');
        const fullscreenShowing = document.getElementById('fullscreenShowing');

        if (fullscreenTotal) fullscreenTotal.textContent = this.client.messages.length.toLocaleString();
        if (fullscreenShowing) fullscreenShowing.textContent = shownCount.toLocaleString();

        if (this.client.fullscreenAutoScroll) {
            setTimeout(() => {
                const messagesContainer = document.getElementById('fullscreenMessagesContainer');
                if (messagesContainer) {
                    messagesContainer.scrollTop = messagesContainer.scrollHeight;
                }
            }, 100);
        }
    }

    toggleFullscreenAutoScroll() {
        this.client.fullscreenAutoScroll = !this.client.fullscreenAutoScroll;
        const statusElement = document.getElementById('fullscreenAutoScrollStatus');
        if (statusElement) {
            statusElement.textContent = this.client.fullscreenAutoScroll ? 'On' : 'Off';
        }

        if (this.client.fullscreenAutoScroll) {
            this.updateFullscreenDisplay();
        }
    }

    setupFullscreenEventListeners() {
        let searchTimeout;

        const fullscreenSearch = document.getElementById('fullscreenSearch');
        if (fullscreenSearch) {
            fullscreenSearch.addEventListener('input', (e) => {
                clearTimeout(searchTimeout);
                searchTimeout = setTimeout(() => {
                    this.client.fullscreenFilters.search = e.target.value;
                    this.updateFullscreenDisplay();
                }, 300);
            });
        }

        const fullscreenSource = document.getElementById('fullscreenSource');
        if (fullscreenSource) {
            fullscreenSource.addEventListener('input', (e) => {
                clearTimeout(searchTimeout);
                searchTimeout = setTimeout(() => {
                    this.client.fullscreenFilters.source = e.target.value;
                    this.updateFullscreenDisplay();
                }, 300);
            });
        }

        const fullscreenSeverity = document.getElementById('fullscreenSeverity');
        if (fullscreenSeverity) {
            fullscreenSeverity.addEventListener('change', (e) => {
                this.client.fullscreenFilters.severity = e.target.value;
                this.updateFullscreenDisplay();
            });
        }

        // Când se schimbă highlight în filtrele normale, actualizează și fullscreen
        const highlightFilter = document.getElementById('highlightFilter');
        if (highlightFilter) {
            highlightFilter.addEventListener('input', (e) => {
                clearTimeout(searchTimeout);
                searchTimeout = setTimeout(() => {
                    // Actualizează fullscreen dacă e deschis
                    const fullscreenContainer = document.getElementById('fullscreenLogs');
                    if (fullscreenContainer && !fullscreenContainer.classList.contains('hidden')) {
                        this.updateFullscreenDisplay();
                    }
                }, 300);
            });
        }

    	const fullscreenHighlight = document.getElementById('fullscreenHighlight');
    	if (fullscreenHighlight) {
    	    fullscreenHighlight.addEventListener('input', (e) => {
    	        clearTimeout(searchTimeout);
    	        searchTimeout = setTimeout(() => {
    	            this.client.fullscreenFilters.highlight = e.target.value;
    	            this.updateFullscreenDisplay();
    	        }, 300);
    	    });
    	}

        const messagesContainer = document.getElementById('fullscreenMessagesContainer');
        if (messagesContainer) {
            messagesContainer.addEventListener('scroll', () => {
                const scrollThreshold = 50;
                const isNearBottom = messagesContainer.scrollHeight - messagesContainer.clientHeight <=
                    messagesContainer.scrollTop + scrollThreshold;

                if (!isNearBottom && this.client.fullscreenAutoScroll) {
                    this.client.fullscreenAutoScroll = false;
                    const statusElement = document.getElementById('fullscreenAutoScrollStatus');
                    if (statusElement) {
                        statusElement.textContent = 'Off';
                    }
                } else if (isNearBottom && !this.client.fullscreenAutoScroll) {
                    this.client.fullscreenAutoScroll = true;
                    const statusElement = document.getElementById('fullscreenAutoScrollStatus');
                    if (statusElement) {
                        statusElement.textContent = 'On';
                    }
                }
            });
        }
    }
}