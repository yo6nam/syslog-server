class UIManager {
    constructor(client) {
        this.client = client;
    }

    renderMessages() {
        const container = document.getElementById('messagesList');
        if (!container) return;

        if (this.client.filteredMessages.length === 0) {
            if (this.client.pagination.isLoading && this.client.pagination.initialLoad) {
                container.innerHTML = '<div class="message" style="text-align: center; color: #888;">Loading messages...</div>';
            } else {
                container.innerHTML = '<div class="message" style="text-align: center; color: #888;">No messages matching filters</div>';
            }
            return;
        }

        container.innerHTML = this.client.filteredMessages.map(msg => {
            const timestamp = this.client.formatTimestamp(msg.timestamp);
            const hostname = msg.hostname || 'unknown';
            const tag = msg.tag || '';

            const severityNumber = msg.severity;
            const severityMap = {
                0: 'emergency',
                1: 'alert',
                2: 'critical',
                3: 'error',
                4: 'warning',
                5: 'notice',
                6: 'info',
                7: 'debug'
            };
            const severity = severityMap[severityNumber] || 'unknown';
            const severityDisplay = severity.toUpperCase();

            const facility = msg.facility_string || this.client.getFacilityString(msg.facility);
            const facilityDisplay = facility.toUpperCase();

            const protocol = msg.protocol || '';
            const protocolPrefix = protocol && protocol !== 'N/A' ? `[${protocol}] ` : '';

            const remoteIp = msg.remote_ip || 'N/A';

            const content = this.client.applyHighlight(msg.content || 'No content', this.client.filters.highlight);
            const rawMessage = msg.raw_message || '';

            let rawMessageHtml = '';
            if (this.client.showRawMessage && rawMessage && rawMessage !== msg.content) {
                const highlightedRawMessage = this.client.applyHighlight(rawMessage, this.client.filters.highlight);
                rawMessageHtml = `<div style="margin-top: 5px; font-size: 11px; color: #888; font-family: monospace;">${highlightedRawMessage}</div>`;
            }

            return `
			<div class="message">
			    <div class="message-header">
			        <span class="timestamp">${timestamp}</span>
			        <span class="hostname clickable-filter" data-type="hostname" data-value="${this.client.escapeHtml(hostname)}" title="Filter by ${hostname}">${hostname}</span>
			        ${tag ? `<span class="tag clickable-filter" data-type="tag" data-value="${this.client.escapeHtml(tag)}" title="Filter by ${tag}">${tag}</span>` : ''}
			        <span class="facility ${facility} clickable-filter" data-type="facility" data-value="${facility}" title="Filter by ${facilityDisplay}">${facilityDisplay}</span>
			        <span class="severity ${severity}">${severityDisplay}</span>
			        <span class="remote-ip clickable-filter" data-type="ip" data-value="${this.client.escapeHtml(remoteIp)}" title="Filter by ${remoteIp}" style="margin-left: auto; color: #888; font-size: 12px;">${protocolPrefix}${remoteIp}</span>
			    </div>
			    <div class="content">${content}</div>
			    ${rawMessageHtml}
			</div>`;
        }).join('');

        this.setupClickableFilters();

        if (this.client.autoScroll && !this.client.pagination.isLoading) {
            setTimeout(() => this.client.scrollToTop(), 100);
        }
    }

    setupClickableFilters() {
        document.querySelectorAll('.clickable-filter').forEach(el => {
            el.addEventListener('click', (e) => {
                e.stopPropagation();
                this.client.filterManager?.applyQuickFilter?.(
                    el.getAttribute('data-type'),
                    el.getAttribute('data-value')
                );
            });
        });
    }

    updateLoadMoreButton() {
        const button = document.getElementById('loadMoreBtn');
        if (!button) return;

        if (this.client.pagination.isLoading) {
            button.disabled = true;
            button.innerHTML = 'Loading... <span class="spinner"></span>';
        } else {
            button.disabled = false;

            if (this.client.pagination.hasMore) {
                if (this.client.pagination.total === 0) {
                    button.textContent = 'Load More';
                } else {
                    const remaining = Math.min(
                        this.client.pagination.total - this.client.messages.length,
                        this.client.pagination.maxMessages - this.client.messages.length
                    );
                    if (remaining > 0) {
                        button.textContent = `Load More (${remaining} remaining)`;
                    } else {
                        button.textContent = `Limit Reached (max ${this.client.pagination.maxMessages})`;
                        button.disabled = true;
                    }
                }
            } else {
                if (this.client.messages.length >= this.client.pagination.maxMessages) {
                    button.textContent = `Limit Reached (max ${this.client.pagination.maxMessages})`;
                } else if (this.client.pagination.total === 0) {
                    button.textContent = 'No Messages Available';
                } else {
                    button.textContent = 'All Messages Loaded';
                }
                button.disabled = true;
            }
        }
    }

    startUptimeCounter() {
        setInterval(() => {
            const uptime = Math.floor((Date.now() - this.client.startTime) / 1000);
            const hours = Math.floor(uptime / 3600);
            const minutes = Math.floor((uptime % 3600) / 60);
            const seconds = uptime % 60;

            const uptimeElement = document.getElementById('uptime');
            if (uptimeElement) {
                uptimeElement.textContent =
                    `${hours.toString().padStart(2, '0')}:${minutes.toString().padStart(2, '0')}:${seconds.toString().padStart(2, '0')}`;
            }
        }, 1000);
    }

    restoreCheckboxState() {
        const checkbox = document.getElementById('showRawMessage');
        if (checkbox) {
            checkbox.checked = this.client.showRawMessage;
        }
    }

    restoreFiltersState() {
        const filtersPanel = document.getElementById('filtersPanel');
        if (filtersPanel) {
            if (this.client.showFilters) {
                filtersPanel.classList.remove('hidden');
            } else {
                filtersPanel.classList.add('hidden');
            }
        }
    }

    toggleFilters() {
        this.client.showFilters = !this.client.showFilters;
        this.client.savePreference('showFilters', this.client.showFilters);

        const filtersPanel = document.getElementById('filtersPanel');
        if (filtersPanel) {
            if (this.client.showFilters) {
                filtersPanel.classList.remove('hidden');
            } else {
                filtersPanel.classList.add('hidden');
            }
        }
    }

    toggleSearch() {
        this.client.showSearch = !this.client.showSearch;
        const searchPanel = document.getElementById('searchPanel');
        if (searchPanel) {
            if (this.client.showSearch) {
                searchPanel.classList.remove('hidden');
                const searchInput = document.getElementById('searchAllInput');
                if (searchInput) searchInput.focus();
            } else {
                searchPanel.classList.add('hidden');
            }
        }
    }
}