class SearchManager {
    constructor(client) {
        this.client = client;
    }

    async performSearch(loadMore = false) {
        const searchTerm = document.getElementById('searchAllInput').value;
        const hostname = document.getElementById('searchAllHostname').value;
        const severity = document.getElementById('searchAllSeverity').value;
        const dateFrom = document.getElementById('searchDateFrom').value;
        const dateTo = document.getElementById('searchDateTo').value;

        if (!loadMore && !searchTerm && !hostname && !severity && !dateFrom && !dateTo) {
            alert('Please enter at least one search criteria');
            return;
        }

        this.client.searchMode = true;
        this.client.pagination.isLoading = true;
        this.client.uiManager?.updateLoadMoreButton?.();

        if (!loadMore) {
            const searchBanner = document.getElementById('searchModeBanner');
            if (searchBanner) searchBanner.classList.remove('hidden');
            this.client.pagination.offset = 0;
        }

        try {
            const params = new URLSearchParams({
                q: searchTerm || '',
                hostname: hostname || '',
                severity: severity || '',
                date_from: dateFrom || '',
                date_to: dateTo || '',
                limit: 100,
                offset: loadMore ? this.client.pagination.offset : 0
            });

            const url = `api/messages/search?${params}`;
            const response = await fetch(url);

            if (response.ok) {
                const data = await response.json();
                const searchResults = data.messages || [];
                const total = data.pagination.total || 0;

                if (loadMore) {
                    this.client.messages = [...this.client.messages, ...searchResults.map(msg => this.client.normalizeMessage(msg))];
                } else {
                    this.client.messages = searchResults.map(msg => this.client.normalizeMessage(msg));
                }

                this.client.pagination.total = total;
                this.client.pagination.offset = this.client.messages.length;
                this.client.pagination.hasMore = this.client.messages.length < total &&
                    this.client.messages.length < this.client.pagination.maxMessages;

                this.client.filterManager?.applyFilters?.();
                this.client.updateStats();
                this.client.updateTimestamps?.();

                this.updateSearchResultsUI(total, dateFrom, dateTo);

                if (!loadMore) {
                    this.updateSearchBanner(searchTerm, hostname, severity, dateFrom, dateTo);
                }
            } else {
                const errorText = await response.text();
                alert('Search failed: ' + errorText);
            }
        } catch (error) {
            console.error('Search error:', error);
            alert('Search failed. Check console for details.');
        } finally {
            this.client.pagination.isLoading = false;
            this.client.uiManager?.updateLoadMoreButton?.();
        }
    }

    updateSearchResultsUI(total, dateFrom, dateTo) {
        let resultsText = `Found ${total} message(s)`;
        if (this.client.messages.length < total) {
            resultsText += ` (showing ${this.client.messages.length})`;
        }
        if (dateFrom || dateTo) {
            resultsText += ` | ${dateFrom || 'start'} to ${dateTo || 'now'}`;
        }

        const searchResultsEl = document.getElementById('searchResults');
        if (searchResultsEl) {
            searchResultsEl.textContent = resultsText;
        }
    }

    updateSearchBanner(searchTerm, hostname, severity, dateFrom, dateTo) {
        const criteriaEl = document.getElementById('searchCriteria');
        if (criteriaEl) {
            let criteria = [];
            if (searchTerm) criteria.push(`Text: "${searchTerm}"`);
            if (hostname) criteria.push(`Host: "${hostname}"`);
            if (severity) criteria.push(`Severity: ${severity}`);
            if (dateFrom || dateTo) criteria.push(`Date: ${dateFrom || 'start'} → ${dateTo || 'now'}`);

            criteriaEl.textContent = criteria.length > 0 ? criteria.join(' | ') : 'Live updates paused';
        }
    }

    clearSearch() {
        this.client.searchMode = false;

        const searchAllInput = document.getElementById('searchAllInput');
        const searchAllHostname = document.getElementById('searchAllHostname');
        const searchAllSeverity = document.getElementById('searchAllSeverity');
        const searchDateFrom = document.getElementById('searchDateFrom');
        const searchDateTo = document.getElementById('searchDateTo');

        if (searchAllInput) searchAllInput.value = '';
        if (searchAllHostname) searchAllHostname.value = '';
        if (searchAllSeverity) searchAllSeverity.value = '';
        if (searchDateFrom) searchDateFrom.value = '';
        if (searchDateTo) searchDateTo.value = '';

        const searchResultsEl = document.getElementById('searchResults');
        if (searchResultsEl) searchResultsEl.textContent = '';

        const searchCriteriaEl = document.getElementById('searchCriteria');
        if (searchCriteriaEl) searchCriteriaEl.textContent = '';

        const searchModeBanner = document.getElementById('searchModeBanner');
        if (searchModeBanner) searchModeBanner.classList.add('hidden');

        this.client.clearMessages();
        this.client.loadMoreMessages();
    }
}