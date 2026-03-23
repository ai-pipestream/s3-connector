<template>
  <main class="container" :class="theme">
    <header class="topbar">
      <div class="topbar-left">
        <h1>S3 Connector</h1>
        <p class="subtitle">Break-glass operator UI (no JDBC-style crawl definitions / streams)</p>
      </div>
      <div class="topbar-right">
        <nav class="tabs">
          <button type="button" :class="{ active: tab === 'dashboard' }" @click="tab = 'dashboard'">Dashboard</button>
          <button type="button" :class="{ active: tab === 'datasources' }" @click="tab = 'datasources'; loadDatasources()">Datasources</button>
          <button type="button" :class="{ active: tab === 'targets' }" @click="tab = 'targets'">Targets</button>
          <button type="button" :class="{ active: tab === 'states' }" @click="tab = 'states'">States</button>
        </nav>
        <button type="button" class="theme-toggle" @click="toggleTheme" :title="theme === 'dark' ? 'Light mode' : 'Dark mode'">
          {{ theme === 'dark' ? '&#9788;' : '&#9790;' }}
        </button>
      </div>
    </header>

    <section class="card settings">
      <h2>Session</h2>
      <div class="form-grid">
        <div class="form-field">
          <label>x-api-key</label>
          <input v-model="apiKey" type="password" autocomplete="off" placeholder="Required for most /api calls" />
        </div>
        <div class="form-field">
          <label>x-datasource-id (default)</label>
          <input v-model="datasourceId" autocomplete="off" placeholder="Optional header for start-crawl" />
        </div>
      </div>
      <p class="hint">Operator-only. Values are stored in <code>localStorage</code> for convenience.</p>
    </section>

    <!-- Dashboard -->
    <template v-if="tab === 'dashboard'">
      <section class="card">
        <h2>Test bucket <span class="tag">no x-api-key</span></h2>
        <p class="hint">Same validation as gRPC <code>testBucketCrawl</code> — connection only.</p>
        <div class="form-field full">
          <label>connectionConfig (JSON)</label>
          <textarea v-model="testForm.connectionConfigJson" rows="6" placeholder='{ "region": "us-east-1", ... }'></textarea>
        </div>
        <div class="form-grid">
          <div class="form-field"><label>bucket</label><input v-model="testForm.bucket" /></div>
          <div class="form-field"><label>prefix</label><input v-model="testForm.prefix" /></div>
          <div class="form-field chk"><label><input type="checkbox" v-model="testForm.dryRun" /> dryRun</label></div>
          <div class="form-field"><label>maxSample</label><input type="number" v-model.number="testForm.maxSample" min="1" /></div>
        </div>
        <button type="button" :disabled="testing" @click="runTestBucket">{{ testing ? 'Testing…' : 'Test bucket' }}</button>
        <pre v-if="testResultJson" class="json-out">{{ testResultJson }}</pre>
      </section>

      <section class="card">
        <h2>Start crawl</h2>
        <p class="hint">Requires <code>x-api-key</code>; <code>datasource_id</code> from body or default header.</p>
        <div class="form-field full">
          <label>connectionConfig (JSON)</label>
          <textarea v-model="startForm.connectionConfigJson" rows="6"></textarea>
        </div>
        <div class="form-grid">
          <div class="form-field"><label>datasourceId</label><input v-model="startForm.datasourceId" :placeholder="datasourceId || '(use header)'" /></div>
          <div class="form-field"><label>bucket</label><input v-model="startForm.bucket" /></div>
          <div class="form-field"><label>prefix</label><input v-model="startForm.prefix" /></div>
          <div class="form-field"><label>requestId</label><input v-model="startForm.requestId" placeholder="optional" /></div>
        </div>
        <button type="button" :disabled="starting || !apiKey" @click="runStartCrawl">{{ starting ? 'Running…' : 'Start crawl' }}</button>
        <pre v-if="startResultJson" class="json-out">{{ startResultJson }}</pre>
      </section>

      <section class="card">
        <h2>Health</h2>
        <a class="link" href="/q/health" target="_blank" rel="noopener">Open <code>/q/health</code></a>
      </section>
    </template>

    <!-- Datasources -->
    <template v-if="tab === 'datasources'">
      <section class="card">
        <h2>Datasources <button type="button" class="small" @click="loadDatasources">Refresh</button></h2>
        <div v-if="!apiKey" class="empty">Set x-api-key to load or save.</div>
        <table v-else class="table">
          <thead><tr><th>ID</th><th>API key</th><th>Actions</th></tr></thead>
          <tbody>
            <tr v-for="ds in datasources" :key="ds.datasourceId">
              <td><code>{{ ds.datasourceId }}</code></td>
              <td class="muted">{{ mask(ds.apiKey) }}</td>
              <td><button type="button" class="small secondary" @click="editDs(ds)">Edit</button></td>
            </tr>
          </tbody>
        </table>

        <div v-if="showDsForm" class="form-card">
          <h3>Edit {{ editingDs.datasourceId }}</h3>
          <div class="form-field full">
            <label>s3Config (JSON)</label>
            <textarea v-model="editingDs.s3ConfigText" rows="12"></textarea>
          </div>
          <div class="form-actions">
            <button type="button" @click="saveDs">Save (PUT)</button>
            <button type="button" class="secondary" @click="showDsForm = false">Cancel</button>
          </div>
        </div>
      </section>
    </template>

    <!-- Targets -->
    <template v-if="tab === 'targets'">
      <section class="card">
        <h2>Crawl targets</h2>
        <div class="row">
          <label>datasourceId</label>
          <input v-model="targetDatasourceId" placeholder="required" />
          <button type="button" class="small" @click="loadTargets">Load</button>
          <button type="button" class="small" @click="openNewTarget">+ New</button>
        </div>
        <table v-if="targets.length" class="table">
          <thead>
            <tr>
              <th>id</th><th>name</th><th>bucket</th><th>prefix</th><th>mode</th><th></th>
            </tr>
          </thead>
          <tbody>
            <tr v-for="t in targets" :key="t.id">
              <td>{{ t.id }}</td>
              <td>{{ t.targetName }}</td>
              <td>{{ t.bucket }}</td>
              <td class="muted">{{ t.objectPrefix || '—' }}</td>
              <td>{{ t.crawlMode }}</td>
              <td>
                <button type="button" class="small secondary" @click="editTarget(t)">Edit</button>
                <button type="button" class="small danger" @click="deleteTarget(t.id)">Delete</button>
              </td>
            </tr>
          </tbody>
        </table>
        <div v-else-if="targetsLoaded" class="empty">No targets</div>

        <div v-if="showTargetForm" class="form-card">
          <h3>{{ targetForm.id ? 'Edit target #' + targetForm.id : 'New target' }}</h3>
          <div class="form-grid">
            <div class="form-field"><label>datasourceId</label><input v-model="targetForm.datasourceId" /></div>
            <div class="form-field"><label>targetName</label><input v-model="targetForm.targetName" /></div>
            <div class="form-field"><label>bucket</label><input v-model="targetForm.bucket" /></div>
            <div class="form-field"><label>objectPrefix</label><input v-model="targetForm.objectPrefix" /></div>
            <div class="form-field">
              <label>mode</label>
              <select v-model="targetForm.mode">
                <option v-for="m in targetModes" :key="m" :value="m">{{ m }}</option>
              </select>
            </div>
            <div class="form-field"><label>failureAllowance</label><input type="number" v-model.number="targetForm.failureAllowance" min="0" /></div>
            <div class="form-field"><label>maxKeysPerRequest</label><input type="number" v-model.number="targetForm.maxKeysPerRequest" min="1" /></div>
          </div>
          <div class="form-actions">
            <button type="button" @click="saveTarget">{{ targetForm.id ? 'Update' : 'Create' }}</button>
            <button type="button" class="secondary" @click="showTargetForm = false">Cancel</button>
          </div>
        </div>
      </section>
    </template>

    <!-- States -->
    <template v-if="tab === 'states'">
      <section class="card">
        <h2>Crawl states</h2>
        <div class="row">
          <label>datasourceId</label>
          <input v-model="stateDatasourceId" placeholder="required" />
          <button type="button" class="small" @click="loadStates">Load</button>
          <button type="button" class="small" @click="showStateForm = true">+ New</button>
        </div>
        <div v-if="states.length" class="table-wrap">
          <table class="table">
            <thead>
              <tr>
                <th>id</th><th>bucket</th><th>key</th><th>status</th><th></th>
              </tr>
            </thead>
            <tbody>
              <tr v-for="s in states" :key="s.id">
                <td>{{ s.id }}</td>
                <td>{{ s.bucket }}</td>
                <td class="ellipsis" :title="s.objectKey">{{ s.objectKey }}</td>
                <td>{{ s.status }}</td>
                <td><button type="button" class="small danger" @click="deleteState(s.id)">Delete</button></td>
              </tr>
            </tbody>
          </table>
        </div>
        <div v-else-if="statesLoaded" class="empty">No states</div>

        <div v-if="showStateForm" class="form-card">
          <h3>New crawl state</h3>
          <div class="form-grid">
            <div class="form-field"><label>datasourceId</label><input v-model="stateForm.datasourceId" /></div>
            <div class="form-field"><label>bucket</label><input v-model="stateForm.bucket" /></div>
            <div class="form-field full"><label>objectKey</label><input v-model="stateForm.objectKey" /></div>
            <div class="form-field"><label>objectVersionId</label><input v-model="stateForm.objectVersionId" placeholder="empty string ok" /></div>
            <div class="form-field"><label>objectEtag</label><input v-model="stateForm.objectEtag" /></div>
            <div class="form-field"><label>sizeBytes</label><input type="number" v-model.number="stateForm.sizeBytes" min="0" /></div>
            <div class="form-field"><label>fingerprint</label><input v-model="stateForm.fingerprint" /></div>
          </div>
          <div class="form-actions">
            <button type="button" @click="createState">Create</button>
            <button type="button" class="secondary" @click="showStateForm = false">Cancel</button>
          </div>
        </div>
      </section>
    </template>
  </main>
</template>

<script>
const API_CONTROL = '/api/control'
const API_DS = '/api/datasources'
const API_TARGETS = '/api/crawl-targets'
const API_STATES = '/api/crawl-states'

export default {
  name: 'App',
  data() {
    return {
      theme: localStorage.getItem('s3-connector-theme') || 'dark',
      tab: 'dashboard',
      apiKey: localStorage.getItem('s3-connector-api-key') || '',
      datasourceId: localStorage.getItem('s3-connector-datasource-id') || '',
      testing: false,
      testForm: {
        connectionConfigJson: '{\n  \n}',
        bucket: '',
        prefix: '',
        dryRun: false,
        maxSample: 100,
      },
      testResultJson: '',
      starting: false,
      startForm: {
        connectionConfigJson: '{\n  \n}',
        datasourceId: '',
        bucket: '',
        prefix: '',
        requestId: '',
      },
      startResultJson: '',
      datasources: [],
      showDsForm: false,
      editingDs: { datasourceId: '', s3ConfigText: '' },
      targetDatasourceId: '',
      targets: [],
      targetsLoaded: false,
      showTargetForm: false,
      targetModes: ['INITIAL', 'INCREMENTAL', 'LIVE', 'BOTH'],
      targetForm: {
        id: null,
        datasourceId: '',
        targetName: '',
        bucket: '',
        objectPrefix: '',
        mode: 'INITIAL',
        failureAllowance: 1,
        maxKeysPerRequest: 1000,
      },
      stateDatasourceId: '',
      states: [],
      statesLoaded: false,
      showStateForm: false,
      stateForm: {
        datasourceId: '',
        bucket: '',
        objectKey: '',
        objectVersionId: '',
        objectEtag: '',
        sizeBytes: 0,
        fingerprint: '',
      },
    }
  },
  watch: {
    apiKey(v) { localStorage.setItem('s3-connector-api-key', v) },
    datasourceId(v) { localStorage.setItem('s3-connector-datasource-id', v) },
  },
  mounted() {
    document.documentElement.setAttribute('data-theme', this.theme)
  },
  methods: {
    toggleTheme() {
      this.theme = this.theme === 'dark' ? 'light' : 'dark'
      localStorage.setItem('s3-connector-theme', this.theme)
      document.documentElement.setAttribute('data-theme', this.theme)
    },
    mask(s) {
      if (!s) return '—'
      return s.length <= 8 ? '****' : `${s.slice(0, 4)}…${s.slice(-4)}`
    },
    baseHeaders(auth) {
      const h = { 'Content-Type': 'application/json' }
      if (auth !== false && this.apiKey) h['x-api-key'] = this.apiKey
      if (this.datasourceId) h['x-datasource-id'] = this.datasourceId
      return h
    },
    async runTestBucket() {
      this.testing = true
      this.testResultJson = ''
      try {
        let connectionConfig
        try {
          connectionConfig = JSON.parse(this.testForm.connectionConfigJson || '{}')
        } catch (e) {
          this.testResultJson = 'Invalid JSON in connectionConfig: ' + e.message
          return
        }
        const body = {
          connectionConfig,
          bucket: this.testForm.bucket,
          prefix: this.testForm.prefix || undefined,
          dryRun: this.testForm.dryRun,
          maxSample: this.testForm.maxSample || 100,
        }
        const res = await fetch(`${API_CONTROL}/test-bucket`, {
          method: 'POST',
          headers: this.baseHeaders(false),
          body: JSON.stringify(body),
        })
        const text = await res.text()
        this.testResultJson = text
      } finally {
        this.testing = false
      }
    },
    async runStartCrawl() {
      this.starting = true
      this.startResultJson = ''
      try {
        let connectionConfig
        try {
          connectionConfig = JSON.parse(this.startForm.connectionConfigJson || '{}')
        } catch (e) {
          this.startResultJson = 'Invalid JSON in connectionConfig: ' + e.message
          return
        }
        const body = {
          connectionConfig,
          bucket: this.startForm.bucket,
          prefix: this.startForm.prefix || undefined,
          requestId: this.startForm.requestId || undefined,
        }
        const ds = this.startForm.datasourceId || this.datasourceId
        if (ds) body.datasourceId = ds
        const res = await fetch(`${API_CONTROL}/start-crawl`, {
          method: 'POST',
          headers: this.baseHeaders(true),
          body: JSON.stringify(body),
        })
        this.startResultJson = await res.text()
      } finally {
        this.starting = false
      }
    },
    async loadDatasources() {
      if (!this.apiKey) return
      const res = await fetch(API_DS, { headers: this.baseHeaders(true) })
      this.datasources = res.ok ? await res.json() : []
    },
    editDs(ds) {
      this.editingDs = {
        datasourceId: ds.datasourceId,
        s3ConfigText: JSON.stringify(ds.s3Config, null, 2),
      }
      this.showDsForm = true
    },
    async saveDs() {
      let connectionConfig
      try {
        connectionConfig = JSON.parse(this.editingDs.s3ConfigText)
      } catch (e) {
        alert('Invalid JSON: ' + e.message)
        return
      }
      const res = await fetch(`${API_DS}/${encodeURIComponent(this.editingDs.datasourceId)}`, {
        method: 'PUT',
        headers: this.baseHeaders(true),
        body: JSON.stringify({ connectionConfig }),
      })
      if (!res.ok) {
        alert(await res.text())
        return
      }
      this.showDsForm = false
      await this.loadDatasources()
    },
    openNewTarget() {
      this.targetForm = {
        id: null,
        datasourceId: this.targetDatasourceId || this.datasourceId,
        targetName: '',
        bucket: '',
        objectPrefix: '',
        mode: 'INITIAL',
        failureAllowance: 1,
        maxKeysPerRequest: 1000,
      }
      this.showTargetForm = true
    },
    editTarget(t) {
      this.targetForm = {
        id: t.id,
        datasourceId: t.datasourceId,
        targetName: t.targetName,
        bucket: t.bucket,
        objectPrefix: t.objectPrefix || '',
        mode: t.crawlMode,
        failureAllowance: t.failureAllowance,
        maxKeysPerRequest: t.maxKeysPerRequest,
      }
      this.showTargetForm = true
    },
    async loadTargets() {
      this.targetsLoaded = false
      this.targets = []
      if (!this.apiKey || !this.targetDatasourceId) return
      const q = new URLSearchParams({ datasourceId: this.targetDatasourceId })
      const res = await fetch(`${API_TARGETS}?${q}`, { headers: this.baseHeaders(true) })
      this.targets = res.ok ? await res.json() : []
      this.targetsLoaded = true
    },
    async saveTarget() {
      const body = { ...this.targetForm }
      delete body.id
      const isNew = !this.targetForm.id
      const url = isNew ? API_TARGETS : `${API_TARGETS}/${this.targetForm.id}`
      const method = isNew ? 'POST' : 'PUT'
      const res = await fetch(url, {
        method,
        headers: this.baseHeaders(true),
        body: JSON.stringify(body),
      })
      if (!res.ok) {
        alert(await res.text())
        return
      }
      this.showTargetForm = false
      await this.loadTargets()
    },
    async deleteTarget(id) {
      if (!confirm('Delete target ' + id + '?')) return
      await fetch(`${API_TARGETS}/${id}`, { method: 'DELETE', headers: this.baseHeaders(true) })
      await this.loadTargets()
    },
    async loadStates() {
      this.statesLoaded = false
      this.states = []
      if (!this.apiKey || !this.stateDatasourceId) return
      const q = new URLSearchParams({ datasourceId: this.stateDatasourceId })
      const res = await fetch(`${API_STATES}?${q}`, { headers: this.baseHeaders(true) })
      this.states = res.ok ? await res.json() : []
      this.statesLoaded = true
    },
    async createState() {
      const body = { ...this.stateForm }
      const res = await fetch(API_STATES, {
        method: 'POST',
        headers: this.baseHeaders(true),
        body: JSON.stringify(body),
      })
      if (!res.ok) {
        alert(await res.text())
        return
      }
      this.showStateForm = false
      await this.loadStates()
    },
    async deleteState(id) {
      if (!confirm('Delete state ' + id + '?')) return
      await fetch(`${API_STATES}/${id}`, { method: 'DELETE', headers: this.baseHeaders(true) })
      await this.loadStates()
    },
  },
}
</script>

<style>
:root, [data-theme="dark"] {
  --bg: #0f1117; --bg2: #1a1d27; --bg3: #12141c; --border: #2a2d3a;
  --text: #e0e0e0; --text2: #aaa; --text3: #666; --link: #7dd3fc;
  --ok: #22c55e; --fail: #ef4444; --blue: #2563eb; --blue2: #1d4ed8;
}
[data-theme="light"] {
  --bg: #f5f5f5; --bg2: #fff; --bg3: #fafafa; --border: #ddd;
  --text: #1a1a1a; --text2: #555; --text3: #999; --link: #2563eb;
  --ok: #16a34a; --fail: #dc2626; --blue: #2563eb; --blue2: #1d4ed8;
}
* { box-sizing: border-box; margin: 0; padding: 0; }
body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; background: var(--bg); color: var(--text); line-height: 1.5; }
.container { max-width: 960px; margin: 0 auto; padding: 20px; }
.topbar { margin-bottom: 20px; display: flex; justify-content: space-between; align-items: flex-start; flex-wrap: wrap; gap: 8px; }
.topbar h1 { font-size: 1.5rem; }
.subtitle { font-size: 0.8rem; color: var(--text3); margin-top: 4px; max-width: 420px; }
.topbar-right { display: flex; align-items: center; gap: 8px; }
.theme-toggle { background: var(--bg3); border: 1px solid var(--border); color: var(--text); padding: 4px 10px; border-radius: 6px; cursor: pointer; font-size: 1.1rem; }
.tabs { display: flex; gap: 4px; flex-wrap: wrap; }
.tabs button { background: var(--bg2); border: 1px solid var(--border); color: var(--text3); padding: 6px 12px; border-radius: 6px 6px 0 0; cursor: pointer; font-size: 0.85rem; }
.tabs button.active { background: var(--border); color: var(--text); }
.card { background: var(--bg2); border: 1px solid var(--border); border-radius: 8px; padding: 20px; margin-bottom: 16px; }
.card h2 { font-size: 1.1rem; margin-bottom: 12px; display: flex; align-items: center; gap: 8px; flex-wrap: wrap; }
.tag { font-size: 0.65rem; font-weight: 600; text-transform: uppercase; color: var(--text3); border: 1px solid var(--border); padding: 2px 6px; border-radius: 4px; }
.hint { font-size: 0.8rem; color: var(--text3); margin-bottom: 10px; }
.settings h2 { font-size: 1rem; }
.form-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 8px; margin-bottom: 8px; }
.form-field { display: flex; flex-direction: column; gap: 4px; margin-bottom: 8px; }
.form-field.full { grid-column: 1 / -1; }
.form-field label { font-size: 0.75rem; color: var(--text3); text-transform: uppercase; letter-spacing: 0.5px; }
.form-field.chk label { text-transform: none; flex-direction: row; align-items: center; gap: 8px; }
input, select, textarea { background: var(--bg); border: 1px solid var(--border); border-radius: 4px; color: var(--text); padding: 6px 10px; font-size: 0.9rem; font-family: inherit; width: 100%; }
textarea { font-family: ui-monospace, monospace; font-size: 0.8rem; }
.row { display: flex; align-items: center; gap: 10px; margin-bottom: 10px; flex-wrap: wrap; }
.row label { min-width: 100px; font-size: 0.85rem; color: var(--text2); }
button { background: var(--blue); color: #fff; border: none; border-radius: 4px; padding: 8px 16px; cursor: pointer; font-size: 0.9rem; margin-top: 8px; }
button:hover:not(:disabled) { background: var(--blue2); }
button:disabled { opacity: 0.5; cursor: not-allowed; }
button.small { padding: 4px 10px; font-size: 0.8rem; margin: 0; }
button.secondary { background: var(--bg3); color: var(--text); border: 1px solid var(--border); }
button.danger { background: var(--fail); }
.json-out { margin-top: 12px; padding: 12px; background: var(--bg3); border-radius: 6px; font-size: 0.75rem; overflow-x: auto; white-space: pre-wrap; word-break: break-word; border: 1px solid var(--border); }
.link { color: var(--link); }
.table { width: 100%; border-collapse: collapse; font-size: 0.85rem; }
.table th, .table td { border: 1px solid var(--border); padding: 6px 8px; text-align: left; }
.table th { background: var(--bg3); }
.muted { color: var(--text3); }
.ellipsis { max-width: 220px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.table-wrap { overflow-x: auto; }
.empty { color: var(--text3); font-style: italic; font-size: 0.85rem; }
.form-card { background: var(--bg3); border: 1px solid var(--border); border-radius: 6px; padding: 16px; margin-top: 16px; }
.form-actions { display: flex; gap: 8px; margin-top: 8px; }
code { background: var(--bg); padding: 2px 6px; border-radius: 3px; font-size: 0.85rem; color: var(--link); }
</style>
