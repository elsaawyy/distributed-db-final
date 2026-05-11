/* ================================================================
   DistribDB Admin Console — script.js
   All API endpoints preserved. Fully modular.
   ================================================================ */

// ─── State ────────────────────────────────────────────────────────────────
let currentDatabase  = null;
let currentTable     = null;
let currentUpdateId  = null;
let columnsArray     = [];
let insertFields     = [];
let updateFields     = [];
let allRecords       = [];
let filteredRecords  = [];
let currentPage      = 1;
let pageSize         = 10;
let sortCol          = null;
let sortDir          = 'asc';
let allDatabases     = [];
let allTables        = [];
let currentPreset    = 'engineering';

// ─── Init ─────────────────────────────────────────────────────────────────
document.addEventListener('DOMContentLoaded', () => {
    startClock();
    loadStatus();
    setInterval(loadStatus, 10000);
    registerKeyboardShortcuts();
    initTransformPreset();
});

// ─── Clock ────────────────────────────────────────────────────────────────
function startClock() {
    function tick() {
        const now = new Date();
        const h = String(now.getHours()).padStart(2, '0');
        const m = String(now.getMinutes()).padStart(2, '0');
        const s = String(now.getSeconds()).padStart(2, '0');
        const el = document.getElementById('clock');
        if (el) el.textContent = `${h}:${m}:${s}`;
    }
    tick();
    setInterval(tick, 1000);
}


// Debug: Check if elements exist on page load
document.addEventListener('DOMContentLoaded', () => {
    console.log('Analytics button:', document.getElementById('analyticsBtn'));
    console.log('Transform button:', document.getElementById('transformBtn'));
    console.log('Analytics result div:', document.getElementById('analyticsResult'));
    console.log('Transform result div:', document.getElementById('transformResult'));
});

// ─── Keyboard Shortcuts ───────────────────────────────────────────────────
function registerKeyboardShortcuts() {
    document.addEventListener('keydown', e => {
        if (e.target.tagName === 'INPUT' || e.target.tagName === 'TEXTAREA' || e.target.tagName === 'SELECT') return;
        if (e.ctrlKey || e.metaKey) {
            if (e.key === 'n') { e.preventDefault(); openCreateDBModal(); }
            if (e.key === 't') { e.preventDefault(); openCreateTableModal(); }
            if (e.key === 'i') { e.preventDefault(); openInsertModal(); }
        }
        if (e.key === 'Escape') closeAllModals();
    });
}

// ─── Panels ───────────────────────────────────────────────────────────────
function showPanel(name) {
    document.querySelectorAll('.panel').forEach(p => p.classList.remove('active'));
    document.querySelectorAll('.sidebar-item').forEach(b => b.classList.remove('active'));

    const panel = document.getElementById('panel-' + name);
    if (panel) panel.classList.add('active');

    document.querySelectorAll('.sidebar-item').forEach(b => {
        if (b.getAttribute('onclick') && b.getAttribute('onclick').includes(`'${name}'`)) {
            b.classList.add('active');
        }
    });

    const labels = { explorer:'Explorer', workers:'Workers', analytics:'Analytics', transform:'Transform', activity:'Activity Log' };
    const bc = document.getElementById('breadcrumb');
    if (bc) bc.innerHTML = `<span class="bc-item">System</span><span class="bc-sep"> / </span><span class="bc-active">${labels[name] || name}</span>`;

    if (window.innerWidth < 768) {
        document.getElementById('sidebar')?.classList.remove('open');
    }
}

function toggleSidebar() {
    const sb = document.getElementById('sidebar');
    if (window.innerWidth < 768) {
        sb?.classList.toggle('open');
    } else {
        sb?.classList.toggle('collapsed');
        document.getElementById('layout')?.classList.toggle('sidebar-collapsed');
    }
}

function toggleTheme() {
    const html = document.documentElement;
    const isDark = html.getAttribute('data-theme') === 'dark';
    html.setAttribute('data-theme', isDark ? 'light' : 'dark');
    document.getElementById('themeToggle').textContent = isDark ? '☽' : '☀';
}

// ─── Status / Load ────────────────────────────────────────────────────────
async function loadStatus() {
    try {
        const res  = await fetch('/status');
        const data = await res.json();
        if (data.success) {
            allDatabases = data.data.databases || [];
            renderDatabases(allDatabases);
            updateDropdowns(allDatabases);
            renderWorkerStatus(data.data.workers);
            updateGlobalStatus(data.data.workers);
        }
    } catch (err) {
        console.error('Failed to load status:', err);
        updateGlobalStatus(null);
    }
}

function updateGlobalStatus(workers) {
    const pill = document.getElementById('globalStatus');
    if (!pill) return;
    const sp = pill.querySelector('span:last-child');
    if (!workers) {
        pill.className = 'status-pill offline';
        sp.textContent = 'Offline';
        return;
    }
    const alive = workers.filter(w => w.alive).length;
    if (alive === workers.length) {
        pill.className = 'status-pill online';
        sp.textContent = `${alive}/${workers.length} nodes online`;
    } else if (alive > 0) {
        pill.className = 'status-pill partial';
        sp.textContent = `${alive}/${workers.length} nodes online`;
    } else {
        pill.className = 'status-pill offline';
        sp.textContent = 'All nodes offline';
    }
}

function renderWorkerStatus(workers) {
    const container = document.getElementById('workerCards');
    if (!container || !workers) return;
    container.innerHTML = workers.map(w => {
        const port = w.address.replace(/https?:\/\/[^:]+:/, '');
        const stateClass = w.alive ? 'alive' : 'dead';
        const stateLabel = w.alive ? 'ONLINE' : 'OFFLINE';
        return `
        <div class="worker-card ${stateClass}">
            <div class="worker-header">
                <span class="worker-name">
                    <span class="worker-dot"></span>Node :${port}
                </span>
                <span class="worker-status-badge">${stateLabel}</span>
            </div>
            <div class="worker-addr">${w.address}</div>
        </div>`;
    }).join('');
}

function renderDatabases(databases) {
    const container = document.getElementById('databasesList');
    const countEl   = document.getElementById('dbCount');
    if (!container) return;
    countEl.textContent = databases.length;
    if (!databases || databases.length === 0) {
        container.innerHTML = `<div class="empty-state"><div class="empty-icon">⬡</div><p>No databases yet</p><button class="btn btn-sm btn-primary" onclick="openCreateDBModal()">Create first DB</button></div>`;
        return;
    }
    container.innerHTML = databases.map(db => `
        <div class="db-item ${currentDatabase === db ? 'selected' : ''}" onclick="selectDatabase('${db}')">
            <span class="item-icon">⬡</span>
            <span class="item-name">${db}</span>
            <button class="item-delete" onclick="event.stopPropagation(); confirmDeleteDatabase('${db}')" title="Delete">✕</button>
        </div>
    `).join('');
}

function filterDatabases(query) {
    const filtered = allDatabases.filter(db => db.toLowerCase().includes(query.toLowerCase()));
    renderDatabases(filtered);
}

function updateDropdowns(databases) {
    ['analyticsDb', 'transformDb'].forEach(id => {
        const sel = document.getElementById(id);
        if (sel) {
            const cur = sel.value;
            sel.innerHTML = '<option value="">Select database…</option>' +
                databases.map(db => `<option value="${db}" ${db === cur ? 'selected' : ''}>${db}</option>`).join('');
        }
    });
}

async function selectDatabase(dbName) {
    currentDatabase = dbName;
    currentTable    = null;
    allRecords      = [];
    filteredRecords = [];

    const bc = document.getElementById('breadcrumb');
    if (bc) bc.innerHTML = `<span class="bc-item">Explorer</span><span class="bc-sep"> / </span><span class="bc-active">${dbName}</span>`;

    document.querySelectorAll('.db-item').forEach(el => el.classList.remove('selected'));
    document.querySelectorAll('.db-item').forEach(el => {
        if (el.querySelector('.item-name')?.textContent === dbName) el.classList.add('selected');
    });

    document.getElementById('recordsContent').innerHTML = `<div class="empty-state"><div class="empty-icon">≡</div><p>Select a table to view records</p></div>`;
    document.getElementById('recordCount').textContent = '0';
    document.getElementById('recordsToolbar').style.display = 'none';
    document.getElementById('pagination').style.display = 'none';
    document.getElementById('exportBtn').style.display = 'none';

    const tablesList = document.getElementById('tablesList');
    tablesList.innerHTML = `<div class="empty-state"><div class="empty-icon">◌</div><p>Loading…</p></div>`;

    try {
        const res  = await fetch(`/list-tables?database=${dbName}`);
        const data = await res.json();
        allTables  = (data.success && data.data) ? data.data : [];
        renderTables(allTables);
    } catch {
        tablesList.innerHTML = `<div class="empty-state"><div class="empty-icon">!</div><p style="color:var(--red)">Error loading tables</p></div>`;
    }
}

function renderTables(tables) {
    const container = document.getElementById('tablesList');
    const countEl   = document.getElementById('tableCount');
    countEl.textContent = tables.length;
    if (!tables || tables.length === 0) {
        container.innerHTML = `<div class="empty-state"><div class="empty-icon">▦</div><p>No tables yet</p><button class="btn btn-sm btn-primary" onclick="openCreateTableModal()">Create table</button></div>`;
        return;
    }
    container.innerHTML = tables.map(t => `
        <div class="table-item ${currentTable === t ? 'selected' : ''}" onclick="selectTable('${t}')">
            <span class="item-icon">▦</span>
            <span class="item-name">${t}</span>
        </div>
    `).join('');

    ['analyticsTable', 'transformTable'].forEach(id => {
        const sel = document.getElementById(id);
        if (sel) {
            sel.innerHTML = '<option value="">Select table…</option>' +
                tables.map(t => `<option value="${t}">${t}</option>`).join('');
        }
    });
}

function filterTables(query) {
    const filtered = allTables.filter(t => t.toLowerCase().includes(query.toLowerCase()));
    renderTables(filtered);
}

async function selectTable(tableName) {
    currentTable = tableName;
    currentPage  = 1;

    document.querySelectorAll('.table-item').forEach(el => el.classList.remove('selected'));
    document.querySelectorAll('.table-item').forEach(el => {
        if (el.querySelector('.item-name')?.textContent === tableName) el.classList.add('selected');
    });

    const bc = document.getElementById('breadcrumb');
    if (bc) bc.innerHTML = `<span class="bc-item">Explorer</span><span class="bc-sep"> / </span><span class="bc-item">${currentDatabase}</span><span class="bc-sep"> / </span><span class="bc-active">${tableName}</span>`;

    document.getElementById('recordsTitle').textContent = tableName;
    document.getElementById('recordsContent').innerHTML = `<div class="empty-state"><div class="empty-icon">◌</div><p>Loading records…</p></div>`;

    try {
        const res  = await fetch(`/select?database=${currentDatabase}&table=${tableName}`);
        const data = await res.json();
        if (data.success && data.data) {
            allRecords      = data.data;
            filteredRecords = [...allRecords];
            renderRecords();
            logActivity('info', `Loaded ${allRecords.length} records from ${currentDatabase}.${tableName}`);
        } else {
            allRecords = filteredRecords = [];
            renderRecords();
        }
    } catch {
        document.getElementById('recordsContent').innerHTML = `<div class="empty-state"><div class="empty-icon">!</div><p style="color:var(--red)">Error loading records</p></div>`;
    }
}

// ─── Records Rendering (sortable, paginated) ───────────────────────────────
function renderRecords() {
    const container = document.getElementById('recordsContent');
    const countEl   = document.getElementById('recordCount');
    const toolbar   = document.getElementById('recordsToolbar');
    const pagination = document.getElementById('pagination');
    const exportBtn  = document.getElementById('exportBtn');

    countEl.textContent = filteredRecords.length;

    if (!filteredRecords || filteredRecords.length === 0) {
        container.innerHTML = `<div class="empty-state"><div class="empty-icon">≡</div><p>${allRecords.length ? 'No records match your search' : 'No records found'}</p><button class="btn btn-sm btn-success" onclick="openInsertModal()">Insert first record</button></div>`;
        toolbar.style.display = 'none';
        pagination.style.display = 'none';
        exportBtn.style.display = 'none';
        return;
    }

    toolbar.style.display = 'flex';
    exportBtn.style.display = '';

    const colSet = new Set(['id']);
    filteredRecords.forEach(r => Object.keys(r.fields || {}).forEach(k => colSet.add(k)));
    const cols = Array.from(colSet);

    let sorted = [...filteredRecords];
    if (sortCol) {
        sorted.sort((a, b) => {
            const av = sortCol === 'id' ? a.id : (a.fields?.[sortCol] ?? '');
            const bv = sortCol === 'id' ? b.id : (b.fields?.[sortCol] ?? '');
            const cmp = String(av).localeCompare(String(bv), undefined, { numeric: true });
            return sortDir === 'asc' ? cmp : -cmp;
        });
    }

    const total = sorted.length;
    const pages = Math.ceil(total / pageSize);
    const start = (currentPage - 1) * pageSize;
    const pageRecords = sorted.slice(start, start + pageSize);

    let html = `<div class="records-table-wrap"><table><thead><tr>`;
    html += cols.map(c => `<th onclick="sortBy('${c}')" class="${sortCol === c ? 'sort-' + sortDir : ''}">${c}</th>`).join('');
    html += `<th>Actions</th></tr></thead><tbody>`;
    pageRecords.forEach(record => {
        html += '<tr>';
        cols.forEach(c => {
            if (c === 'id') {
                html += `<td class="id-cell" title="${record.id}">${record.id.substring(0,8)}…</td>`;
            } else {
                const val = record.fields?.[c] ?? '';
                html += `<td title="${String(val)}">${String(val)}</td>`;
            }
        });
        html += `<td class="action-cell">
                    <button class="btn-icon-sm btn-icon-edit" onclick='openUpdateModal("${record.id}", ${JSON.stringify(record.fields || {})})' title="Edit">✎</button>
                    <button class="btn-icon-sm btn-icon-del" onclick="confirmDeleteRecord('${record.id}')" title="Delete">⊗</button>
                 </td>`;
        html += '</tr>';
    });
    html += `</tbody></table></div>`;
    container.innerHTML = html;

    if (pages > 1) {
        pagination.style.display = 'flex';
        const pageRange = buildPageRange(currentPage, pages);
        pagination.innerHTML = `
            <span class="page-info">${start + 1}–${Math.min(start + pageSize, total)} of ${total}</span>
            <div class="page-btns">
                <button class="page-btn" onclick="goPage(${currentPage - 1})" ${currentPage === 1 ? 'disabled' : ''}>‹</button>
                ${pageRange.map(p => p === '…' ? `<span class="page-btn" style="cursor:default">…</span>` : `<button class="page-btn ${p === currentPage ? 'active' : ''}" onclick="goPage(${p})">${p}</button>`).join('')}
                <button class="page-btn" onclick="goPage(${currentPage + 1})" ${currentPage === pages ? 'disabled' : ''}>›</button>
            </div>`;
    } else {
        pagination.style.display = 'none';
    }
}

function buildPageRange(current, total) {
    if (total <= 7) return Array.from({ length: total }, (_, i) => i + 1);
    const pages = [];
    if (current <= 4) {
        for (let i = 1; i <= 5; i++) pages.push(i);
        pages.push('…', total);
    } else if (current >= total - 3) {
        pages.push(1, '…');
        for (let i = total - 4; i <= total; i++) pages.push(i);
    } else {
        pages.push(1, '…', current - 1, current, current + 1, '…', total);
    }
    return pages;
}

function goPage(p) {
    const pages = Math.ceil(filteredRecords.length / pageSize);
    if (p < 1 || p > pages) return;
    currentPage = p;
    renderRecords();
}

function sortBy(col) {
    if (sortCol === col) {
        sortDir = sortDir === 'asc' ? 'desc' : 'asc';
    } else {
        sortCol = col;
        sortDir = 'asc';
    }
    currentPage = 1;
    renderRecords();
}

function changePageSize(val) {
    pageSize = parseInt(val);
    currentPage = 1;
    renderRecords();
}

function filterRecords(query) {
    const q = query.toLowerCase();
    filteredRecords = allRecords.filter(r => {
        if (r.id.toLowerCase().includes(q)) return true;
        return Object.values(r.fields || {}).some(v => String(v).toLowerCase().includes(q));
    });
    currentPage = 1;
    renderRecords();
}

function exportCSV() {
    if (!allRecords.length) { showToast('No records to export', 'warn'); return; }
    const colSet = new Set(['id']);
    allRecords.forEach(r => Object.keys(r.fields || {}).forEach(k => colSet.add(k)));
    const cols = Array.from(colSet);
    const rows = [cols.join(',')];
    allRecords.forEach(r => {
        rows.push(cols.map(c => {
            const v = c === 'id' ? r.id : (r.fields?.[c] ?? '');
            return `"${String(v).replace(/"/g, '""')}"`;
        }).join(','));
    });
    const blob = new Blob([rows.join('\n')], { type: 'text/csv' });
    const a = document.createElement('a');
    a.href = URL.createObjectURL(blob);
    a.download = `${currentDatabase}_${currentTable}_export.csv`;
    a.click();
    logActivity('success', `Exported ${currentTable} to CSV`);
}

// ─── Database CRUD ────────────────────────────────────────────────────────
function openCreateDBModal() {
    document.getElementById('dbName').value = '';
    document.getElementById('dbNameError').textContent = '';
    document.getElementById('createDBModal').style.display = 'flex';
    setTimeout(() => document.getElementById('dbName').focus(), 50);
}
function closeCreateDBModal() { document.getElementById('createDBModal').style.display = 'none'; }

async function createDatabase() {
    const name = document.getElementById('dbName').value.trim();
    const errEl = document.getElementById('dbNameError');
    if (!name) { errEl.textContent = 'Database name is required.'; return; }
    if (!/^[a-z0-9_]+$/.test(name)) { errEl.textContent = 'Use lowercase letters, numbers, and underscores only.'; return; }
    errEl.textContent = '';
    try {
        const res = await fetch('/create-db', { method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({ name }) });
        if (res.ok) {
            showToast(`Database "${name}" created`, 'success');
            logActivity('success', `Created database: ${name}`);
            closeCreateDBModal();
            loadStatus();
        } else {
            const err = await res.json();
            errEl.textContent = err.error || 'Failed to create database.';
        }
    } catch { errEl.textContent = 'Network error. Please try again.'; }
}

function confirmDeleteDatabase(name) {
    showConfirm('Delete Database', `Delete "${name}"? All tables and data will be permanently lost.`, () => deleteDatabase(name));
}

async function deleteDatabase(name) {
    try {
        const res = await fetch('/drop-db', { method:'DELETE', headers:{'Content-Type':'application/json'}, body: JSON.stringify({ name }) });
        if (res.ok) {
            showToast(`Database "${name}" deleted`, 'success');
            logActivity('warn', `Deleted database: ${name}`);
            if (currentDatabase === name) {
                currentDatabase = null; currentTable = null;
                allRecords = filteredRecords = [];
                document.getElementById('tablesList').innerHTML = `<div class="empty-state"><div class="empty-icon">▦</div><p>Select a database first</p></div>`;
                document.getElementById('recordsContent').innerHTML = `<div class="empty-state"><div class="empty-icon">≡</div><p>Select a table to view records</p></div>`;
                document.getElementById('recordsToolbar').style.display = 'none';
                document.getElementById('pagination').style.display = 'none';
            }
            loadStatus();
        } else {
            const err = await res.json();
            showToast(err.error || 'Delete failed', 'error');
        }
    } catch { showToast('Network error', 'error'); }
}

// ─── Table CRUD ───────────────────────────────────────────────────────────
function openCreateTableModal() {
    if (!currentDatabase) { showToast('Select a database first', 'warn'); return; }
    columnsArray = [];
    document.getElementById('tableName').value = '';
    document.getElementById('tableNameError').textContent = '';
    renderColumns();
    document.getElementById('createTableModal').style.display = 'flex';
    setTimeout(() => document.getElementById('tableName').focus(), 50);
}
function closeCreateTableModal() { document.getElementById('createTableModal').style.display = 'none'; }

function addColumn() { columnsArray.push({ name:'', type:'string', required:false }); renderColumns(); }
function removeColumn(idx) { columnsArray.splice(idx,1); renderColumns(); }

const COLUMN_TEMPLATES = {
    user:    [
        { name:'name', type:'string', required:true },
        { name:'email', type:'string', required:true },
        { name:'age', type:'int', required:false }
    ],
    product: [
        { name:'title', type:'string', required:true },
        { name:'price', type:'float', required:true },
        { name:'stock', type:'int', required:false },
        { name:'category', type:'string', required:false }
    ],
    order:   [
        { name:'user_id', type:'string', required:true },
        { name:'total', type:'float', required:true },
        { name:'status', type:'string', required:false }
    ],
    log:     [
        { name:'level', type:'string', required:true },
        { name:'message', type:'string', required:true },
        { name:'source', type:'string', required:false }
    ]
};

function addTemplateColumns(tpl) {
    const template = COLUMN_TEMPLATES[tpl];
    if (!template) return;
    columnsArray = [...template];
    renderColumns();
}

function renderColumns() {
    const container = document.getElementById('columnsContainer');
    if (columnsArray.length === 0) {
        container.innerHTML = `<div class="empty-state" style="padding:20px"><p>Click "+ Add Column" or choose a template above.</p></div>`;
        return;
    }
    container.innerHTML = columnsArray.map((col, idx) => `
        <div class="column-row">
            <input type="text" class="input" placeholder="Column name" value="${col.name}" oninput="columnsArray[${idx}].name = this.value" style="flex:2">
            <select class="input select-type" style="flex:1.2" onchange="columnsArray[${idx}].type = this.value">
                ${['string','int','float','bool'].map(t => `<option value="${t}" ${col.type === t ? 'selected' : ''}>${t}</option>`).join('')}
            </select>
            <label class="col-required"><input type="checkbox" ${col.required ? 'checked' : ''} onchange="columnsArray[${idx}].required = this.checked"> Req</label>
            <button class="btn btn-sm btn-ghost" onclick="removeColumn(${idx})" title="Remove">✕</button>
        </div>
    `).join('');
}

async function createTable() {
    const tableName = document.getElementById('tableName').value.trim();
    const errEl = document.getElementById('tableNameError');
    if (!tableName) { errEl.textContent = 'Table name is required.'; return; }
    if (columnsArray.length === 0) { errEl.textContent = 'Add at least one column.'; return; }
    if (columnsArray.some(c => !c.name.trim())) { errEl.textContent = 'All columns must have a name.'; return; }
    errEl.textContent = '';

    const columns = columnsArray.map(c => ({ name: c.name.trim(), type: c.type, required: c.required }));

    try {
        const res = await fetch('/create-table', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ database: currentDatabase, table: tableName, columns })
        });
        if (res.ok) {
            showToast(`Table "${tableName}" created`, 'success');
            logActivity('success', `Created table: ${currentDatabase}.${tableName}`);
            closeCreateTableModal();
            selectDatabase(currentDatabase);
        } else {
            const err = await res.json();
            errEl.textContent = err.error || 'Failed to create table.';
        }
    } catch { errEl.textContent = 'Network error. Please try again.'; }
}

// ─── Insert Record ────────────────────────────────────────────────────────
function openInsertModal() {
    if (!currentDatabase || !currentTable) { showToast('Select a database and table first', 'warn'); return; }
    insertFields = [];
    document.getElementById('insertTableName').innerHTML = `→ ${currentTable}`;
    document.getElementById('insertFieldsContainer').innerHTML = `<div id="insertFieldsForm"></div><button class="btn btn-sm btn-ghost" onclick="addInsertField()" style="margin-top:8px">+ Add Field</button>`;
    renderInsertFields();
    document.getElementById('insertModal').style.display = 'flex';
}

function addInsertField() { insertFields.push({ name:'', value:'' }); renderInsertFields(); }
function removeInsertField(idx) { insertFields.splice(idx,1); renderInsertFields(); }

function renderInsertFields() {
    const container = document.getElementById('insertFieldsForm');
    if (!container) return;
    if (insertFields.length === 0) {
        container.innerHTML = `<div class="empty-state" style="padding:16px"><p>Click "+ Add Field" to add data fields.</p></div>`;
        return;
    }
    container.innerHTML = insertFields.map((f, idx) => `
        <div class="field-row">
            <input type="text" class="input" placeholder="Field name" value="${f.name}" oninput="insertFields[${idx}].name = this.value">
            <input type="text" class="input" placeholder="Value" value="${f.value}" oninput="insertFields[${idx}].value = this.value">
            <button class="btn btn-sm btn-ghost" onclick="removeInsertField(${idx})">✕</button>
        </div>
    `).join('');
}

function closeInsertModal() { document.getElementById('insertModal').style.display = 'none'; insertFields = []; }

async function insertRecord() {
    const fields = {};
    for (const f of insertFields) if (f.name.trim()) fields[f.name.trim()] = f.value;
    if (!Object.keys(fields).length) { showToast('Add at least one field', 'warn'); return; }
    try {
        const res = await fetch('/insert', { method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({ database: currentDatabase, table: currentTable, fields }) });
        if (res.ok) {
            showToast('Record inserted', 'success');
            logActivity('success', `Inserted record into ${currentDatabase}.${currentTable}`);
            closeInsertModal();
            selectTable(currentTable);
        } else {
            const err = await res.json();
            showToast(err.error || 'Insert failed', 'error');
        }
    } catch { showToast('Network error', 'error'); }
}

// ─── Update Record ────────────────────────────────────────────────────────
function openUpdateModal(id, fields) {
    currentUpdateId = id;
    updateFields = Object.entries(fields).map(([k,v]) => ({ name:k, value:v }));
    document.getElementById('updateFieldsContainer').innerHTML = `<div id="updateFieldsForm"></div><button class="btn btn-sm btn-ghost" onclick="addUpdateField()" style="margin-top:8px">+ Add Field</button>`;
    renderUpdateFields();
    document.getElementById('updateModal').style.display = 'flex';
}

function addUpdateField() { updateFields.push({ name:'', value:'' }); renderUpdateFields(); }
function removeUpdateField(idx) { updateFields.splice(idx,1); renderUpdateFields(); }

function renderUpdateFields() {
    const container = document.getElementById('updateFieldsForm');
    if (!container) return;
    if (updateFields.length === 0) {
        container.innerHTML = `<div class="empty-state" style="padding:16px"><p>No fields to update.</p></div>`;
        return;
    }
    container.innerHTML = updateFields.map((f, idx) => `
        <div class="field-row">
            <input type="text" class="input" placeholder="Field name" value="${f.name}" oninput="updateFields[${idx}].name = this.value">
            <input type="text" class="input" placeholder="New value" value="${String(f.value)}" oninput="updateFields[${idx}].value = this.value">
            <button class="btn btn-sm btn-ghost" onclick="removeUpdateField(${idx})">✕</button>
        </div>
    `).join('');
}

function closeUpdateModal() { document.getElementById('updateModal').style.display = 'none'; }

async function updateRecord() {
    const fields = {};
    for (const f of updateFields) if (f.name.trim()) fields[f.name.trim()] = f.value;
    if (!Object.keys(fields).length) { showToast('Add at least one field to update', 'warn'); return; }
    try {
        const res = await fetch('/update', { method:'PUT', headers:{'Content-Type':'application/json'}, body: JSON.stringify({ database: currentDatabase, table: currentTable, id: currentUpdateId, fields }) });
        if (res.ok) {
            showToast('Record updated', 'success');
            logActivity('success', `Updated record in ${currentDatabase}.${currentTable}`);
            closeUpdateModal();
            selectTable(currentTable);
        } else {
            const err = await res.json();
            showToast(err.error || 'Update failed', 'error');
        }
    } catch { showToast('Network error', 'error'); }
}

function confirmDeleteRecord(id) {
    showConfirm('Delete Record', `Permanently delete record ${id.substring(0,8)}…?`, () => deleteRecord(id));
}

async function deleteRecord(id) {
    try {
        const res = await fetch('/delete', { method:'DELETE', headers:{'Content-Type':'application/json'}, body: JSON.stringify({ database: currentDatabase, table: currentTable, id }) });
        if (res.ok) {
            showToast('Record deleted', 'success');
            logActivity('warn', `Deleted record from ${currentDatabase}.${currentTable}`);
            selectTable(currentTable);
        } else {
            showToast('Delete failed', 'error');
        }
    } catch { showToast('Network error', 'error'); }
}

// ─── Analytics ────────────────────────────────────────────────────────────
async function loadAnalyticsTables() {
    const db = document.getElementById('analyticsDb').value;
    if (!db) return;
    const res = await fetch(`/list-tables?database=${db}`);
    const data = await res.json();
    const sel = document.getElementById('analyticsTable');
    sel.innerHTML = '<option value="">Select table…</option>' + (data.data || []).map(t => `<option value="${t}">${t}</option>`).join('');
}

async function runAnalytics() {
    const db = document.getElementById('analyticsDb').value;
    const table = document.getElementById('analyticsTable').value;
    if (!db || !table) { 
        showToast('Select database and table', 'warn'); 
        return; 
    }
    
    const btn = document.getElementById('analyticsBtn');
    if (!btn) {
        console.error('analyticsBtn not found');
        showToast('UI Error: Button not found', 'error');
        return;
    }
    
    btn.disabled = true;
    btn.innerHTML = '<span class="spinner"></span> Running…';
    
    const resultBox = document.getElementById('analyticsResult');
    const resultPre = document.getElementById('analyticsResultPre');
    
    if (resultBox) resultBox.style.display = 'block';
    if (resultPre) resultPre.textContent = 'Loading…';
    
    try {
        const res = await fetch(`/proxy/analytics?database=${db}&table=${table}`);
        const data = await res.json();
        if (resultPre) resultPre.textContent = JSON.stringify(data, null, 2);
        logActivity('success', `Analytics ran on ${db}.${table}`);
    } catch (err) {
        if (resultPre) resultPre.textContent = 'Error: ' + err.message;
        logActivity('error', `Analytics failed: ${err.message}`);
    }
    
    btn.disabled = false;
    btn.innerHTML = 'Run Analytics';
}

// ─── Transform ────────────────────────────────────────────────────────────
const TRANSFORM_PRESETS = {
    engineering: [ { type:'filter', column:'department', operator:'eq', value:'Engineering' }, { type:'project', columns:['name','salary'] }, { type:'sort', column:'salary', order:'desc' } ],
    topN: [ { type:'sort', column:'id', order:'asc' }, { type:'limit', n:10 } ],
    custom: []
};

function initTransformPreset() { setPreset('engineering', document.querySelector('.preset-btn')); }

function setPreset(name, btn) {
    currentPreset = name;
    document.querySelectorAll('#panel-transform .preset-btn').forEach(b => b.classList.remove('active'));
    if (btn) btn.classList.add('active');
    const ta = document.getElementById('transformJson');
    if (ta) {
        ta.value = JSON.stringify(TRANSFORM_PRESETS[name] || [], null, 2);
        ta.readOnly = (name !== 'custom');
    }
}

async function loadTransformTables() {
    const db = document.getElementById('transformDb').value;
    if (!db) return;
    const res = await fetch(`/list-tables?database=${db}`);
    const data = await res.json();
    const sel = document.getElementById('transformTable');
    sel.innerHTML = '<option value="">Select table…</option>' + (data.data || []).map(t => `<option value="${t}">${t}</option>`).join('');
}

async function runTransform() {
    const db = document.getElementById('transformDb').value;
    const table = document.getElementById('transformTable').value;
    if (!db || !table) { 
        showToast('Select database and table', 'warn'); 
        return; 
    }
    
    let transformations;
    try { 
        transformations = JSON.parse(document.getElementById('transformJson').value || '[]'); 
    } catch { 
        showToast('Invalid JSON in transformations', 'error'); 
        return; 
    }
    
    const btn = document.getElementById('transformBtn');
    if (!btn) {
        console.error('transformBtn not found');
        showToast('UI Error: Button not found', 'error');
        return;
    }
    
    btn.disabled = true;
    btn.innerHTML = '<span class="spinner"></span> Running…';
    
    const resultBox = document.getElementById('transformResult');
    const resultPre = document.getElementById('transformResultPre');
    
    if (resultBox) resultBox.style.display = 'block';
    if (resultPre) resultPre.textContent = 'Loading…';
    
    try {
        const res = await fetch('/proxy/transform', { 
            method: 'POST', 
            headers: { 'Content-Type': 'application/json' }, 
            body: JSON.stringify({ database: db, table, transformations }) 
        });
        const data = await res.json();
        if (resultPre) resultPre.textContent = JSON.stringify(data, null, 2);
        logActivity('success', `Transform ran on ${db}.${table}`);
    } catch (err) {
        if (resultPre) resultPre.textContent = 'Error: ' + err.message;
        logActivity('error', `Transform failed: ${err.message}`);
    }
    
    btn.disabled = false;
    btn.innerHTML = 'Run Transform';
}
function copyResult(boxId) {
    const pre = document.getElementById(boxId + 'Pre');
    if (!pre) return;
    navigator.clipboard.writeText(pre.textContent).then(() => showToast('Copied to clipboard', 'success'));
}

// ─── Activity Log ─────────────────────────────────────────────────────────
const activityLog = [];
function logActivity(type, message) {
    const now = new Date();
    const time = `${String(now.getHours()).padStart(2,'0')}:${String(now.getMinutes()).padStart(2,'0')}:${String(now.getSeconds()).padStart(2,'0')}`;
    activityLog.unshift({ type, message, time });
    if (activityLog.length > 100) activityLog.pop();
    renderActivityLog();
}
function renderActivityLog() {
    const container = document.getElementById('activityLog');
    if (!container) return;
    if (!activityLog.length) { container.innerHTML = `<div class="empty-state"><div class="empty-icon">≡</div><p>No activity yet</p></div>`; return; }
    const labels = { success:'OK', error:'ERR', info:'INFO', warn:'WARN' };
    container.innerHTML = activityLog.map(e => `<div class="log-entry"><span class="log-time">${e.time}</span><span class="log-badge log-${e.type}">${labels[e.type] || e.type}</span><span class="log-message">${e.message}</span></div>`).join('');
}
function clearLog() { activityLog.length = 0; renderActivityLog(); }

// ─── Toast & Confirm ──────────────────────────────────────────────────────
function showToast(message, type = 'info') {
    const icons = { success:'✓', error:'✕', warn:'⚠', info:'ℹ' };
    const container = document.getElementById('toastContainer');
    const toast = document.createElement('div');
    toast.className = `toast toast-${type}`;
    toast.innerHTML = `<span class="toast-icon">${icons[type] || 'ℹ'}</span><span>${message}</span>`;
    container.appendChild(toast);
    setTimeout(() => { toast.classList.add('hiding'); setTimeout(() => toast.remove(), 300); }, 3200);
}

function showConfirm(title, message, onConfirm) {
    document.getElementById('confirmTitle').textContent = title;
    document.getElementById('confirmMessage').textContent = message;
    const dialog = document.getElementById('confirmDialog');
    dialog.style.display = 'flex';
    const okBtn = document.getElementById('confirmOk'); const cancelBtn = document.getElementById('confirmCancel');
    const cleanup = () => { dialog.style.display = 'none'; };
    okBtn.onclick = () => { cleanup(); onConfirm(); };
    cancelBtn.onclick = cleanup;
}

function closeAllModals() {
    ['createDBModal','createTableModal','insertModal','updateModal'].forEach(id => {
        const el = document.getElementById(id);
        if (el) el.style.display = 'none';
    });
}
document.addEventListener('click', e => { if (e.target.classList.contains('modal-overlay')) closeAllModals(); });