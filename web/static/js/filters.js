class FilterManager {
    constructor(client) {
        this.client = client;
    }

    matchesFilters(msg) {
        const searchText = this.client.filters.search.toLowerCase();
        const matchesSearch = !this.client.filters.search ||
            (msg.content && msg.content.toLowerCase().includes(searchText)) ||
            (msg.raw_message && msg.raw_message.toLowerCase().includes(searchText)) ||
            (msg.tag && msg.tag.toLowerCase().includes(searchText));

        const matchesHostname = !this.client.filters.hostname ||
            (msg.hostname && msg.hostname.toLowerCase().includes(this.client.filters.hostname.toLowerCase()));

        let matchesSeverity = true;
        if (this.client.filters.severity) {
            if (this.client.filters.severity.startsWith('range:')) {
                const rangeStr = this.client.filters.severity.replace('range:', '');
                const [minStr, maxStr] = rangeStr.split('-');
                const minSev = parseInt(minStr);
                const maxSev = parseInt(maxStr);

                matchesSeverity = msg.severity >= minSev && msg.severity <= maxSev;
            } else {
                const severityMap = {
                    'emergency': 0, 'alert': 1, 'critical': 2, 'error': 3,
                    'warning': 4, 'notice': 5, 'info': 6, 'debug': 7
                };
                const severityNumber = severityMap[this.client.filters.severity];
                matchesSeverity = msg.severity === severityNumber;
            }
        }

        const msgFacility = (msg.facility_string || this.client.getFacilityString(msg.facility)).toLowerCase();
        const matchesFacility = !this.client.filters.facility ||
            msgFacility === this.client.filters.facility.toLowerCase();

        const matchesIP = !this.client.filters.ip ||
            (msg.remote_ip && msg.remote_ip.includes(this.client.filters.ip));

        return matchesSearch && matchesHostname && matchesSeverity && matchesFacility && matchesIP;
    }

    applyFilters() {
        this.client.filteredMessages = this.client.messages.filter(msg => this.matchesFilters(msg));

        this.client.uiManager?.renderMessages?.();
    }

    clearFilters() {
        // Resetează toate input-urile
        const searchFilter = document.getElementById('searchFilter');
        const hostnameFilter = document.getElementById('hostnameFilter');
        const highlightFilter = document.getElementById('highlightFilter');
        const severityFilter = document.getElementById('severityFilter');

        if (searchFilter) searchFilter.value = '';
        if (hostnameFilter) hostnameFilter.value = '';
        if (highlightFilter) highlightFilter.value = '';
        if (severityFilter) severityFilter.value = '';

        // Resetează filtrele în client
        this.client.filters.search = '';
        this.client.filters.hostname = '';
        this.client.filters.highlight = '';
        this.client.filters.severity = '';
        this.client.filters.facility = '';
        this.client.filters.ip = '';

        this.applyFilters();
        this.client.updateTimestamps?.();
    }

    applyQuickFilter(type, value) {
        if (!this.client.showFilters) {
            this.client.uiManager?.toggleFilters?.();
        }

        this.client.filters.search = '';
        this.client.filters.hostname = '';
        this.client.filters.severity = '';
        this.client.filters.facility = '';
        this.client.filters.ip = '';
        this.client.filters.highlight = '';

        const searchFilter = document.getElementById('searchFilter');
        const hostnameFilter = document.getElementById('hostnameFilter');
        const severityFilter = document.getElementById('severityFilter');
        const highlightFilter = document.getElementById('highlightFilter');

        if (searchFilter) searchFilter.value = '';
        if (hostnameFilter) hostnameFilter.value = '';
        if (severityFilter) severityFilter.value = '';
        if (highlightFilter) highlightFilter.value = '';

        switch(type) {
            case 'hostname':
                if (hostnameFilter) hostnameFilter.value = value;
                this.client.filters.hostname = value;
                break;
            case 'tag':
                if (searchFilter) searchFilter.value = value;
                this.client.filters.search = value;
                break;
            case 'facility':
                this.client.filters.facility = value;
                if (searchFilter) searchFilter.value = `facility:${value}`;
                break;
            case 'ip':
                this.client.filters.ip = value;
                if (searchFilter) searchFilter.value = `ip:${value}`;
                break;
        }

        this.applyFilters();
        this.client.scrollToTop();
        this.showToast(`Filtered by ${type}: "${value}"`);
    }

    showToast(message) {
        const toast = document.createElement('div');
        toast.style.cssText = `
            position: fixed;
            bottom: 20px;
            right: 20px;
            background: #4caf50;
            color: white;
            padding: 12px 20px;
            border-radius: 4px;
            z-index: 10000;
            animation: slideIn 0.3s ease;
        `;
        toast.textContent = message;
        document.body.appendChild(toast);

        setTimeout(() => {
            toast.style.animation = 'slideOut 0.3s ease';
            setTimeout(() => toast.remove(), 300);
        }, 2000);
    }
}