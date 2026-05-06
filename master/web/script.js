let currentDatabase = null;
let currentTable = null;
let currentUpdateId = null;
let columnsArray = [];
let insertFields = [];
let updateFields = [];

async function loadStatus() {
    try {
        const res = await fetch('/status');
        const data = await res.json();
        if (data.success) {
            renderDatabases(data.data.databases);
            updateDropdowns(data.data.databases);
            renderWorkerStatus(data.data.workers);
        }
    } catch (err) {
        console.error('Failed to load status:', err);
    }
}

function renderWorkerStatus(workers) {
    const container = document.getElementById('workerStatus');
    if (!workers) return;
    container.innerHTML = workers.map(w => `
        <span class="px-3 py-1 rounded-full ${w.alive ? 'bg-green-200 text-green-800' : 'bg-red-200 text-red-800'}">
            ${w.address.replace('http://localhost:', '')} ${w.alive ? 'GREEN' : 'RED'}
        </span>
    `).join('');
}

function renderDatabases(databases) {
    const container = document.getElementById('databasesList');
    if (!databases || databases.length === 0) {
        container.innerHTML = '<p class="text-gray-500 text-sm">No databases</p>';
        return;
    }
    container.innerHTML = databases.map(db => `
        <div class="flex justify-between items-center p-2 bg-gray-50 rounded cursor-pointer hover:bg-gray-100 mb-1"
             onclick="selectDatabase('${db}')">
            <span>FOLDER ${db}</span>
            <button onclick="event.stopPropagation(); deleteDatabase('${db}')" class="text-red-500 hover:text-red-700">X</button>
        </div>
    `).join('');
}

function updateDropdowns(databases) {
    const dbSelects = ['analyticsDb', 'transformDb'];
    dbSelects.forEach(id => {
        const select = document.getElementById(id);
        if (select) {
            select.innerHTML = '<option value="">Select Database</option>' + 
                databases.map(db => `<option value="${db}">${db}</option>`).join('');
        }
    });
}

async function selectDatabase(dbName) {
    currentDatabase = dbName;
    document.getElementById('tablesList').innerHTML = '<p class="text-gray-500 text-sm">Loading tables...</p>';
    
    try {
        const res = await fetch(`/list-tables?database=${dbName}`);
        const data = await res.json();
        if (data.success && data.data.length > 0) {
            renderTables(data.data);
        } else {
            document.getElementById('tablesList').innerHTML = '<p class="text-gray-500 text-sm">No tables yet. Click "+ New Table" to create one.</p>';
        }
    } catch (err) {
        document.getElementById('tablesList').innerHTML = '<p class="text-red-500 text-sm">Error loading tables</p>';
    }
}

function renderTables(tables) {
    const container = document.getElementById('tablesList');
    container.innerHTML = tables.map(table => `
        <div class="flex justify-between items-center p-2 bg-gray-50 rounded cursor-pointer hover:bg-gray-100 mb-1"
             onclick="selectTable('${table}')">
            <span>TABLE ${table}</span>
        </div>
    `).join('');
    
    ['analyticsTable', 'transformTable'].forEach(id => {
        const select = document.getElementById(id);
        select.innerHTML = '<option value="">Select Table</option>' + 
            tables.map(t => `<option value="${t}">${t}</option>`).join('');
    });
}

async function selectTable(tableName) {
    currentTable = tableName;
    const res = await fetch(`/select?database=${currentDatabase}&table=${tableName}`);
    const data = await res.json();
    
    if (data.success && data.data) {
        if (data.data.length === 0) {
            document.getElementById('recordsContent').innerHTML = '<p class="text-gray-500 text-sm">No records found</p>';
            return;
        }
        
        const allKeys = new Set();
        data.data.forEach(record => {
            Object.keys(record.fields).forEach(k => allKeys.add(k));
        });
        allKeys.add('id');
        const columns = Array.from(allKeys);
        
        let html = '<div class="overflow-x-auto"><table class="min-w-full text-sm border">';
        html += '<thead><tr class="bg-gray-100">';
        columns.forEach(col => {
            html += `<th class="border p-2 text-left">${col}</th>`;
        });
        html += '<th class="border p-2">Actions</th>';

        html += '</tr></thead><tbody>';
        
        data.data.forEach(record => {
            html += '<tr>';
            columns.forEach(col => {
                if (col === 'id') {
                    html += `<td class="border p-2 font-mono text-xs">${record.id}</td>`;
                } else {
                    const val = record.fields[col] !== undefined ? record.fields[col] : '';
                    html += `<td class="border p-2">${val}</td>`;
                }
            });
            html += `<td class="border p-2">
                        <button onclick='openUpdateModal("${record.id}", ${JSON.stringify(record.fields)})' class="text-blue-500 hover:text-blue-700">Update</button>
                        <button onclick="deleteRecord('${record.id}')" class="text-red-500 hover:text-red-700 ml-2">Delete</button>
                       </td>`;
            html += '</tr>';
        });
        html += '</tbody></table></div>';
        
        document.getElementById('recordsContent').innerHTML = html;
    }
}

// Database functions
function openCreateDBModal() {
    document.getElementById('createDBModal').style.display = 'block';
    document.getElementById('dbName').value = '';
}

function closeCreateDBModal() {
    document.getElementById('createDBModal').style.display = 'none';
}

async function createDatabase() {
    const name = document.getElementById('dbName').value.trim();
    if (!name) {
        alert('Please enter a database name');
        return;
    }
    const res = await fetch('/create-db', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ name })
    });
    if (res.ok) {
        alert('Database created');
        closeCreateDBModal();
        loadStatus();
    } else {
        const err = await res.json();
        alert('Failed: ' + (err.error || 'Unknown error'));
    }
}

async function deleteDatabase(name) {
    if (!confirm(`Delete database "${name}"? All tables and data will be lost.`)) return;
    const res = await fetch('/drop-db', {
        method: 'DELETE',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ name })
    });
    if (res.ok) {
        alert('Database deleted');
        if (currentDatabase === name) {
            currentDatabase = null;
            currentTable = null;
            document.getElementById('tablesList').innerHTML = '<p class="text-gray-500 text-sm">Select a database first</p>';
            document.getElementById('recordsContent').innerHTML = '<p class="text-gray-500 text-sm">Select a table to view records</p>';
        }
        loadStatus();
    } else {
        const err = await res.json();
        alert('Failed: ' + (err.error || 'Unknown error'));
    }
}

// Table functions
function openCreateTableModal() {
    if (!currentDatabase) {
        alert('Please select a database first');
        return;
    }
    columnsArray = [];
    document.getElementById('createTableModal').style.display = 'block';
    document.getElementById('tableName').value = '';
    renderColumns();
}

function closeCreateTableModal() {
    document.getElementById('createTableModal').style.display = 'none';
}

function addColumn() {
    columnsArray.push({ name: '', type: 'string', required: false });
    renderColumns();
}

function removeColumn(index) {
    columnsArray.splice(index, 1);
    renderColumns();
}

function renderColumns() {
    const container = document.getElementById('columnsContainer');
    if (columnsArray.length === 0) {
        container.innerHTML = '<p class="text-gray-500 text-sm">No columns. Click "+ Add Column" to add one.</p>';
        return;
    }
    container.innerHTML = columnsArray.map((col, idx) => `
        <div class="column-row">
            <input type="text" placeholder="Column Name" value="${col.name}" 
                   onchange="columnsArray[${idx}].name = this.value" style="flex:2">
            <select onchange="columnsArray[${idx}].type = this.value" style="flex:1">
                <option value="string" ${col.type === 'string' ? 'selected' : ''}>String</option>
                <option value="int" ${col.type === 'int' ? 'selected' : ''}>Integer</option>
                <option value="float" ${col.type === 'float' ? 'selected' : ''}>Float</option>
                <option value="bool" ${col.type === 'bool' ? 'selected' : ''}>Boolean</option>
            </select>
            <label style="display: flex; align-items: center; gap: 5px;">
                <input type="checkbox" ${col.required ? 'checked' : ''} 
                       onchange="columnsArray[${idx}].required = this.checked"> Required
            </label>
            <button onclick="removeColumn(${idx})" class="btn-remove">Remove</button>
        </div>
    `).join('');
}

async function createTable() {
    const tableName = document.getElementById('tableName').value.trim();
    if (!tableName) {
        alert('Please enter a table name');
        return;
    }
    if (columnsArray.length === 0) {
        alert('Please add at least one column');
        return;
    }
    for (let col of columnsArray) {
        if (!col.name.trim()) {
            alert('All columns must have a name');
            return;
        }
    }
    
    const res = await fetch('/create-table', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
            database: currentDatabase,
            table: tableName,
            columns: columnsArray
        })
    });
    if (res.ok) {
        alert('Table created');
        closeCreateTableModal();
        selectDatabase(currentDatabase);
    } else {
        const err = await res.json();
        alert('Failed: ' + (err.error || 'Unknown error'));
    }
}

// Insert functions
function openInsertModal() {
    if (!currentDatabase) {
        alert('Please select a database first');
        return;
    }
    if (!currentTable) {
        alert('Please select a table first');
        return;
    }
    
    document.getElementById('insertTableName').innerText = currentTable;
    insertFields = [];
    document.getElementById('insertFieldsContainer').innerHTML = `
        <div id="insertFieldsForm"></div>
        <button onclick="addInsertField()" class="btn-add text-sm">+ Add Field</button>
    `;
    renderInsertFields();
    document.getElementById('insertModal').style.display = 'block';
}

function addInsertField() {
    insertFields.push({ name: '', value: '' });
    renderInsertFields();
}

function removeInsertField(index) {
    insertFields.splice(index, 1);
    renderInsertFields();
}

function renderInsertFields() {
    const container = document.getElementById('insertFieldsForm');
    if (!container) return;
    if (insertFields.length === 0) {
        container.innerHTML = '<p class="text-gray-500 text-sm">Click "+ Add Field" to add fields</p>';
        return;
    }
    container.innerHTML = insertFields.map((field, idx) => `
        <div class="column-row">
            <input type="text" placeholder="Field Name" value="${field.name}" 
                   onchange="insertFields[${idx}].name = this.value" style="flex:1">
            <input type="text" placeholder="Value" value="${field.value}" 
                   onchange="insertFields[${idx}].value = this.value" style="flex:2">
            <button onclick="removeInsertField(${idx})" class="btn-remove">Remove</button>
        </div>
    `).join('');
}

function closeInsertModal() {
    document.getElementById('insertModal').style.display = 'none';
    insertFields = [];
}

async function insertRecord() {
    const fields = {};
    for (let field of insertFields) {
        if (field.name.trim()) {
            fields[field.name.trim()] = field.value;
        }
    }
    if (Object.keys(fields).length === 0) {
        alert('Please add at least one field');
        return;
    }
    
    const res = await fetch('/insert', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
            database: currentDatabase,
            table: currentTable,
            fields: fields
        })
    });
    if (res.ok) {
        alert('Record inserted');
        closeInsertModal();
        selectTable(currentTable);
    } else {
        const err = await res.json();
        alert('Failed: ' + (err.error || 'Unknown error'));
    }
}

// Update functions
function openUpdateModal(id, fields) {
    currentUpdateId = id;
    updateFields = Object.entries(fields).map(([key, val]) => ({ name: key, value: val }));
    document.getElementById('updateFieldsContainer').innerHTML = `
        <div id="updateFieldsForm"></div>
        <button onclick="addUpdateField()" class="btn-add text-sm">+ Add Field</button>
    `;
    renderUpdateFields();
    document.getElementById('updateModal').style.display = 'block';
}

function addUpdateField() {
    updateFields.push({ name: '', value: '' });
    renderUpdateFields();
}

function removeUpdateField(index) {
    updateFields.splice(index, 1);
    renderUpdateFields();
}

function renderUpdateFields() {
    const container = document.getElementById('updateFieldsForm');
    if (updateFields.length === 0) {
        container.innerHTML = '<p class="text-gray-500 text-sm">Click "+ Add Field" to add fields to update</p>';
        return;
    }
    container.innerHTML = updateFields.map((field, idx) => `
        <div class="column-row">
            <input type="text" placeholder="Field Name" value="${field.name}" 
                   onchange="updateFields[${idx}].name = this.value" style="flex:1">
            <input type="text" placeholder="New Value" value="${field.value}" 
                   onchange="updateFields[${idx}].value = this.value" style="flex:2">
            <button onclick="removeUpdateField(${idx})" class="btn-remove">Remove</button>
        </div>
    `).join('');
}

function closeUpdateModal() {
    document.getElementById('updateModal').style.display = 'none';
}

async function updateRecord() {
    const fields = {};
    for (let field of updateFields) {
        if (field.name.trim()) {
            fields[field.name.trim()] = field.value;
        }
    }
    if (Object.keys(fields).length === 0) {
        alert('Please add at least one field to update');
        return;
    }
    
    const res = await fetch('/update', {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
            database: currentDatabase,
            table: currentTable,
            id: currentUpdateId,
            fields: fields
        })
    });
    if (res.ok) {
        alert('Record updated');
        closeUpdateModal();
        selectTable(currentTable);
    } else {
        const err = await res.json();
        alert('Failed: ' + (err.error || 'Unknown error'));
    }
}

// Delete function
async function deleteRecord(id) {
    if (!confirm('Delete this record?')) return;
    const res = await fetch('/delete', {
        method: 'DELETE',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
            database: currentDatabase,
            table: currentTable,
            id: id
        })
    });
    if (res.ok) {
        alert('Record deleted');
        selectTable(currentTable);
    } else {
        alert('Delete failed');
    }
}

// Special tasks
async function runAnalytics() {
    const db = document.getElementById('analyticsDb').value;
    const table = document.getElementById('analyticsTable').value;
    if (!db || !table) {
        alert('Select database and table');
        return;
    }
    const resultDiv = document.getElementById('analyticsResult');
    resultDiv.classList.remove('hidden');
    resultDiv.textContent = 'Loading...';
    try {
        const res = await fetch(`/proxy/analytics?database=${db}&table=${table}`);
        const data = await res.json();
        resultDiv.textContent = JSON.stringify(data, null, 2);
    } catch (err) {
        resultDiv.textContent = 'Error: ' + err.message;
    }
}

async function runTransform() {
    const db = document.getElementById('transformDb').value;
    const table = document.getElementById('transformTable').value;
    if (!db || !table) {
        alert('Select database and table');
        return;
    }
    const resultDiv = document.getElementById('transformResult');
    resultDiv.classList.remove('hidden');
    resultDiv.textContent = 'Loading...';
    try {
        const res = await fetch('/proxy/transform', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                database: db,
                table: table,
                transformations: [
                    {"type": "filter", "column": "department", "operator": "eq", "value": "Engineering"},
                    {"type": "project", "columns": ["name", "salary"]},
                    {"type": "sort", "column": "salary", "order": "desc"}
                ]
            })
        });
        const data = await res.json();
        resultDiv.textContent = JSON.stringify(data, null, 2);
    } catch (err) {
        resultDiv.textContent = 'Error: ' + err.message;
    }
}

// Initial load
loadStatus();
setInterval(loadStatus, 10000);