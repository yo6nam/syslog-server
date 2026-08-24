class SearchManager {
    constructor(client) {
        this.client = client;
        this._lastCriteria = null;
        this._lastDateLabel = null;
    }

    async performSearch(loadMore = false) {
        let criteria;

        if (loadMore && this._lastCriteria) {
            // "Load more" refolosește EXACT criteriile ultimei căutări, nu
            // recitește input-urile din DOM — esențial pentru căutări ca
            // "Last 24h", al căror interval nu poate fi reprezentat de
            // selectoarele <input type="date"> (doar granularitate de zi).
            criteria = this._lastCriteria;
        } else {
            criteria = {
                searchTerm: document.getElementById('searchAllInput').value,
                hostname: document.getElementById('searchAllHostname').value,
                severity: document.getElementById('searchAllSeverity').value,
                dateFrom: document.getElementById('searchDateFrom').value,
                dateTo: document.getElementById('searchDateTo').value,
            };

            if (!criteria.searchTerm && !criteria.hostname && !criteria.severity && !criteria.dateFrom && !criteria.dateTo) {
                alert('Please enter at least one search criteria');
                return;
            }
        }

        await this.executeSearch(criteria, loadMore, loadMore ? this._lastDateLabel : null);
    }

    // Comoditate: ultimele 24h, fereastră glisantă (nu aliniată la miezul
    // nopții) — folosește timestamp-uri precise RFC3339, nu selectoarele de
    // dată din UI (acelea acceptă doar granularitate de zi).
    //
    // Cere dintr-o dată până la maxMessages (5000), nu doar 500 — la volum
    // mare de trafic pe 24h, un singur "Load More" de 500 lăsa userul cu
    // impresia (greșită) că a văzut tot. Dacă tot mai există mai multe decât
    // maxMessages, afișăm un avertisment vizibil (nu doar text discret).
    async searchLast24Hours() {
        const now = new Date();
        const yesterday = new Date(now.getTime() - 24 * 60 * 60 * 1000);

        // Golim selectoarele de dată din UI ca să nu sugereze un interval
        // greșit (o zi anume) dacă userul se uită la ele după aceea.
        const searchDateFrom = document.getElementById('searchDateFrom');
        const searchDateTo = document.getElementById('searchDateTo');
        if (searchDateFrom) searchDateFrom.value = '';
        if (searchDateTo) searchDateTo.value = '';

        const criteria = {
            searchTerm: '',
            hostname: '',
            severity: '',
            dateFrom: yesterday.toISOString(),
            dateTo: now.toISOString(),
            limit: this.client.pagination.maxMessages,
        };

        await this.executeSearch(criteria, false, 'Last 24 hours');
    }

    async executeSearch(criteria, loadMore, dateLabel) {
        const { searchTerm, hostname, severity, dateFrom, dateTo } = criteria;
        const limit = criteria.limit || 500;

        this.client.searchMode = true;
        this.client.pagination.isLoading = true;
        this.client.uiManager?.updateLoadMoreButton?.();

        if (!loadMore) {
            const searchBanner = document.getElementById('searchModeBanner');
            if (searchBanner) searchBanner.classList.remove('hidden');
            this.client.pagination.offset = 0;
            this._lastCriteria = criteria;
            this._lastDateLabel = dateLabel || null;
        }

        try {
            const params = new URLSearchParams({
                q: searchTerm || '',
                hostname: hostname || '',
                severity: severity || '',
                date_from: dateFrom || '',
                date_to: dateTo || '',
                limit: limit,
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

                this.updateSearchResultsUI(total, dateLabel, dateFrom, dateTo);
                this.updateSearchLimitWarning(total, dateLabel);

                if (!loadMore) {
                    this.updateSearchBanner(searchTerm, hostname, severity, dateLabel, dateFrom, dateTo);
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

    updateSearchResultsUI(total, dateLabel, dateFrom, dateTo) {
        let resultsText = `Found ${total} message(s)`;
        if (this.client.messages.length < total) {
            resultsText += ` (showing ${this.client.messages.length})`;
        }
        if (dateLabel) {
            resultsText += ` | ${dateLabel}`;
        } else if (dateFrom || dateTo) {
            resultsText += ` | ${dateFrom || 'start'} to ${dateTo || 'now'}`;
        }

        const searchResultsEl = document.getElementById('searchResults');
        if (searchResultsEl) {
            searchResultsEl.textContent = resultsText;
        }
    }

    // Avertisment vizibil (nu doar text discret) când s-a atins plafonul
    // maxMessages și tot mai există mesaje neafișate — asta e diferit de
    // cazul normal "click Load More pentru mai multe", unde userul poate
    // acționa; aici a atins limita absolută afișabilă în UI.
    updateSearchLimitWarning(total) {
        const warningEl = document.getElementById('searchLimitWarning');
        if (!warningEl) return;

        const shown = this.client.messages.length;
        const ceiling = this.client.pagination.maxMessages;

        if (shown >= ceiling && total > ceiling) {
            warningEl.textContent = `⚠ Showing the most recent ${shown} of ${total} matching messages — display limit is ${ceiling}. Narrow the date range or filters to see the rest.`;
            warningEl.classList.remove('hidden');
        } else {
            warningEl.classList.add('hidden');
        }
    }

    updateSearchBanner(searchTerm, hostname, severity, dateLabel, dateFrom, dateTo) {
        const criteriaEl = document.getElementById('searchCriteria');
        if (criteriaEl) {
            let criteria = [];
            if (searchTerm) criteria.push(`Text: "${searchTerm}"`);
            if (hostname) criteria.push(`Host: "${hostname}"`);
            if (severity) criteria.push(`Severity: ${severity}`);
            if (dateLabel) {
                criteria.push(`Date: ${dateLabel}`);
            } else if (dateFrom || dateTo) {
                criteria.push(`Date: ${dateFrom || 'start'} → ${dateTo || 'now'}`);
            }

            criteriaEl.textContent = criteria.length > 0 ? criteria.join(' | ') : 'Live updates paused';
        }
    }

    clearSearch() {
        this.client.searchMode = false;
        this._lastCriteria = null;
        this._lastDateLabel = null;

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

        const searchLimitWarning = document.getElementById('searchLimitWarning');
        if (searchLimitWarning) searchLimitWarning.classList.add('hidden');

        this.client.clearMessages();
        this.client.loadMoreMessages();
    }
}
