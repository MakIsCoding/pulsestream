'use strict'

// ─── State ─────────────────────────────────────────────────────────────────────
const state = {
  token: localStorage.getItem('ps_token') || null,
  user: null,
  topics: [],
  currentTopicId: null,
  mentions: [],
  mentionsTotal: 0,
  mentionsOffset: 0,
  ws: null,
  wsConnected: false,
  newMentionIds: new Set(),
  stats: { topic_count: 0, mention_count: 0 },
  trendChart: null,
  wsUrl: null,
}

// ─── Runtime Config ────────────────────────────────────────────────────────────
async function fetchConfig() {
  try {
    const data = await fetch('/api/config').then(r => r.json())
    state.wsUrl = data.ws_url
  } catch {
    state.wsUrl = `ws://${window.location.hostname}:8001`
  }
}

// ─── Heartbeat ─────────────────────────────────────────────────────────────────
let _heartbeatTimer = null

async function sendHeartbeat() {
  if (!state.token) return
  api('/auth/heartbeat', { method: 'POST' }).catch(() => {})
}

function startHeartbeat() {
  sendHeartbeat()
  if (_heartbeatTimer) clearInterval(_heartbeatTimer)
  _heartbeatTimer = setInterval(sendHeartbeat, 5 * 60 * 1000)
}

function stopHeartbeat() {
  if (_heartbeatTimer) { clearInterval(_heartbeatTimer); _heartbeatTimer = null }
}

// Re-send heartbeat immediately when user comes back to the tab
document.addEventListener('visibilitychange', () => {
  if (!document.hidden && state.token) sendHeartbeat()
})

// ─── API Helper ────────────────────────────────────────────────────────────────
async function api(path, opts = {}) {
  const headers = { 'Content-Type': 'application/json', ...(opts.headers || {}) }
  if (state.token) headers['Authorization'] = `Bearer ${state.token}`
  const res = await fetch(path, { ...opts, headers })
  if (res.status === 401) { logout(); return null }
  if (!res.ok) {
    const err = await res.json().catch(() => ({ detail: res.statusText }))
    throw new Error(err.detail || 'Request failed')
  }
  if (res.status === 204) return null
  return res.json()
}

// ─── Auth ──────────────────────────────────────────────────────────────────────
async function login(email, password) {
  const data = await api('/auth/login', {
    method: 'POST',
    body: JSON.stringify({ email, password }),
  })
  if (!data) return
  state.token = data.access_token
  localStorage.setItem('ps_token', state.token)
  await Promise.all([loadUser(), fetchTopics(), fetchStats(), fetchConfig()])
  navigate('dashboard')
  startHeartbeat()
  connectWs()
}

async function register(email, password) {
  const data = await api('/auth/register', {
    method: 'POST',
    body: JSON.stringify({ email, password }),
  })
  if (!data) return
  state.token = data.access_token
  localStorage.setItem('ps_token', state.token)
  await Promise.all([loadUser(), fetchTopics(), fetchStats(), fetchConfig()])
  navigate('dashboard')
  startHeartbeat()
  connectWs()
}

function logout() {
  stopHeartbeat()
  state.token = null
  state.user = null
  state.topics = []
  state.currentTopicId = null
  state.stats = { topic_count: 0, mention_count: 0 }
  localStorage.removeItem('ps_token')
  disconnectWs()
  navigate('login')
}

async function loadUser() {
  state.user = await api('/auth/me')
}

// ─── Topics ────────────────────────────────────────────────────────────────────
async function fetchTopics() {
  const data = await api('/topics')
  state.topics = data || []
}

async function createTopic(name, keywords) {
  const topic = await api('/topics', {
    method: 'POST',
    body: JSON.stringify({ name, keywords }),
  })
  if (topic) {
    state.topics.unshift(topic)
    state.stats.topic_count++
  }
  return topic
}

async function deleteTopic(id) {
  await api(`/topics/${id}`, { method: 'DELETE' })
  state.topics = state.topics.filter(t => t.id !== id)
  state.stats.topic_count = Math.max(0, state.stats.topic_count - 1)
}

async function toggleTopicPause(id) {
  const topic = state.topics.find(t => t.id === id)
  if (!topic) return
  const endpoint = topic.is_active ? `/topics/${id}/pause` : `/topics/${id}/resume`
  try {
    const updated = await api(endpoint, { method: 'PATCH' })
    if (updated) {
      const idx = state.topics.findIndex(t => t.id === id)
      if (idx !== -1) state.topics[idx] = updated
      document.getElementById('topics-grid').innerHTML = renderTopicsGrid()
      showToast(
        updated.is_active ? 'Topic resumed' : 'Topic paused',
        `"${updated.name}" ingestion ${updated.is_active ? 'resumed' : 'paused'}`
      )
    }
  } catch (e) {
    showToast('Error', e.message)
  }
}

// ─── Stats ─────────────────────────────────────────────────────────────────────
async function fetchStats() {
  const data = await api('/auth/me/stats')
  if (data) state.stats = data
}

function formatStats() {
  const { topic_count: t, mention_count: m } = state.stats
  if (!t && !m) return ''
  return `${t} topic${t !== 1 ? 's' : ''} · ${m.toLocaleString()} mention${m !== 1 ? 's' : ''} stored`
}

// ─── Mentions ──────────────────────────────────────────────────────────────────
async function fetchMentions(topicId, reset = false) {
  const offset = reset ? 0 : state.mentionsOffset
  const url = `/mentions?topic_id=${topicId}&limit=20&offset=${offset}&only_analyzed=false`
  const data = await api(url)
  if (!data) return
  if (reset) {
    state.mentions = data.items
    state.mentionsOffset = data.items.length
    state.mentionsTotal = data.total
  } else {
    state.mentions = [...state.mentions, ...data.items]
    state.mentionsOffset += data.items.length
    state.mentionsTotal = data.total
  }
}

async function fetchDigest(topicId) {
  return api(`/topics/${topicId}/digest/latest`)
}

async function fetchTrend(topicId, bucket = 'hour', window = '24h') {
  return api(`/topics/${topicId}/trend?bucket=${bucket}&window=${window}`)
}

// ─── WebSocket ─────────────────────────────────────────────────────────────────
function connectWs() {
  if (state.ws || !state.token) return
  const url = `${state.wsUrl}/ws?token=${state.token}`
  const ws = new WebSocket(url)

  ws.onopen = () => {
    state.wsConnected = true
    updateWsIndicator(true)
  }

  ws.onmessage = evt => {
    try {
      const msg = JSON.parse(evt.data)
      if (msg.type === 'mention.analyzed') handleLiveMention(msg.data)
    } catch { /* ignore parse errors */ }
  }

  ws.onclose = () => {
    state.wsConnected = false
    state.ws = null
    updateWsIndicator(false)
    if (state.token) setTimeout(connectWs, 5000)
  }

  ws.onerror = () => ws.close()
  state.ws = ws
}

function disconnectWs() {
  if (!state.ws) return
  state.ws.onclose = null
  state.ws.close()
  state.ws = null
  state.wsConnected = false
}

function handleLiveMention(mention) {
  if (state.currentTopicId === mention.topic_id) {
    state.mentions.unshift({
      id: mention.mention_id,
      topic_id: mention.topic_id,
      source: mention.source,
      title: mention.title,
      summary: mention.summary,
      sentiment_score: mention.sentiment_score,
      sentiment_label: mention.sentiment_label,
      entities: mention.entities || [],
      url: null,
      author: null,
      ingested_at: new Date().toISOString(),
      analyzed_at: new Date().toISOString(),
    })
    state.mentionsTotal++
    state.newMentionIds.add(mention.mention_id)
    renderMentionsFeed()
  }
  const topicName = (state.topics.find(t => t.id === mention.topic_id) || {}).name || 'a topic'
  showToast(`New mention — ${topicName}`, mention.title || '(no title)')
}

function updateWsIndicator(connected) {
  const el = document.getElementById('ws-indicator')
  if (!el) return
  el.innerHTML = connected
    ? '<span class="w-2 h-2 rounded-full bg-green-400 pulse-dot"></span><span class="text-green-400">Live</span>'
    : '<span class="w-2 h-2 rounded-full bg-slate-600"></span><span class="text-slate-500">Offline</span>'
}

// ─── Toasts ────────────────────────────────────────────────────────────────────
function showToast(title, body) {
  const container = document.getElementById('toasts')
  if (!container) return
  const el = document.createElement('div')
  el.className = 'fade-in pointer-events-auto bg-slate-800 border border-slate-700 rounded-xl px-4 py-3 shadow-2xl max-w-xs'
  el.innerHTML = `
    <p class="text-xs font-semibold text-indigo-400">${escHtml(title)}</p>
    <p class="text-xs text-slate-400 mt-0.5 truncate">${escHtml(body)}</p>
  `
  container.appendChild(el)
  setTimeout(() => el.classList.add('opacity-0', 'transition-opacity', 'duration-500'), 3500)
  setTimeout(() => el.remove(), 4000)
}

// ─── Router ────────────────────────────────────────────────────────────────────
function navigate(route, params = {}) {
  if (route === 'login') {
    renderLoginPage()
  } else if (route === 'dashboard') {
    renderDashboardPage()
  } else if (route === 'topic') {
    renderTopicPage(params.id)
  }
}

// ─── Utils ─────────────────────────────────────────────────────────────────────
function escHtml(s) {
  if (s == null) return ''
  return String(s)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
}

function timeAgo(iso) {
  if (!iso) return ''
  const diff = Date.now() - new Date(iso).getTime()
  const m = Math.floor(diff / 60000)
  if (m < 1) return 'just now'
  if (m < 60) return `${m}m ago`
  const h = Math.floor(m / 60)
  if (h < 24) return `${h}h ago`
  return `${Math.floor(h / 24)}d ago`
}

function sentimentChip(label, score) {
  if (!label) return ''
  const map = {
    positive: 'bg-green-500/15 text-green-400 border-green-500/25',
    negative: 'bg-red-500/15 text-red-400 border-red-500/25',
    neutral:  'bg-slate-700 text-slate-400 border-slate-600',
  }
  const cls = map[label] || map.neutral
  const num = score != null ? ` ${score >= 0 ? '+' : ''}${score.toFixed(2)}` : ''
  return `<span class="inline-flex items-center gap-1 px-2 py-0.5 rounded border text-xs font-medium ${cls}">${escHtml(label)}${escHtml(num)}</span>`
}

function sourceChip(src) {
  const map = {
    hackernews: 'bg-orange-500/15 text-orange-400 border-orange-500/25',
    reddit:     'bg-red-500/15 text-red-400 border-red-500/25',
  }
  const cls = map[src] || 'bg-slate-700 text-slate-400 border-slate-600'
  return `<span class="inline-flex items-center px-2 py-0.5 rounded border text-xs ${cls}">${escHtml(src)}</span>`
}

// ─── Page: Login / Register ────────────────────────────────────────────────────
function renderLoginPage(mode = 'login') {
  document.getElementById('app').innerHTML = `
    <div class="min-h-screen flex flex-col items-center justify-center p-4 bg-gradient-to-b from-slate-900 to-slate-950">
      <div class="mb-8 text-center">
        <div class="flex items-center justify-center gap-2.5 mb-3">
          <div class="w-9 h-9 rounded-xl bg-indigo-600 flex items-center justify-center shadow-lg">
            <svg class="w-5 h-5 text-white" fill="none" viewBox="0 0 24 24" stroke="currentColor">
              <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2.5" d="M13 10V3L4 14h7v7l9-11h-7z"/>
            </svg>
          </div>
          <span class="text-2xl font-bold tracking-tight text-white">PulseStream</span>
        </div>
        <p class="text-slate-500 text-sm">Real-time topic intelligence</p>
      </div>

      <div class="w-full max-w-sm">
        <div class="flex rounded-xl overflow-hidden border border-slate-700 mb-5 bg-slate-800/50">
          <button onclick="renderLoginPage('login')"
            class="flex-1 py-2.5 text-sm font-medium transition-colors ${mode === 'login' ? 'bg-indigo-600 text-white' : 'text-slate-400 hover:text-white'}">
            Sign In
          </button>
          <button onclick="renderLoginPage('register')"
            class="flex-1 py-2.5 text-sm font-medium transition-colors ${mode === 'register' ? 'bg-indigo-600 text-white' : 'text-slate-400 hover:text-white'}">
            Register
          </button>
        </div>

        <div class="bg-slate-800 border border-slate-700 rounded-2xl p-6 shadow-2xl">
          <form id="auth-form" onsubmit="handleAuthSubmit(event,'${mode}')">
            <div class="space-y-4">
              <div>
                <label class="block text-xs font-medium text-slate-400 mb-1.5">Email address</label>
                <input id="auth-email" type="email" required autocomplete="email"
                  placeholder="you@example.com"
                  class="w-full bg-slate-900/80 border border-slate-700 rounded-lg px-3.5 py-2.5 text-sm text-white placeholder-slate-600 focus:outline-none focus:ring-2 focus:ring-indigo-500 focus:border-transparent transition" />
              </div>
              <div>
                <label class="block text-xs font-medium text-slate-400 mb-1.5">Password</label>
                <input id="auth-password" type="password" required
                  ${mode === 'register' ? 'minlength="8"' : ''}
                  autocomplete="${mode === 'login' ? 'current-password' : 'new-password'}"
                  placeholder="${mode === 'register' ? 'At least 8 characters' : '••••••••'}"
                  class="w-full bg-slate-900/80 border border-slate-700 rounded-lg px-3.5 py-2.5 text-sm text-white placeholder-slate-600 focus:outline-none focus:ring-2 focus:ring-indigo-500 focus:border-transparent transition" />
              </div>
            </div>

            <div id="auth-error" class="hidden mt-4 text-xs text-red-400 bg-red-500/10 border border-red-500/20 rounded-lg px-3 py-2.5"></div>

            <button id="auth-btn" type="submit"
              class="mt-5 w-full bg-indigo-600 hover:bg-indigo-500 active:bg-indigo-700 text-white font-semibold py-2.5 rounded-xl text-sm transition-colors focus:outline-none focus:ring-2 focus:ring-indigo-500 focus:ring-offset-2 focus:ring-offset-slate-800 disabled:opacity-60">
              ${mode === 'login' ? 'Sign In' : 'Create Account'}
            </button>
          </form>
        </div>
      </div>
    </div>
  `
  document.getElementById('auth-email').focus()
}

async function handleAuthSubmit(evt, mode) {
  evt.preventDefault()
  const email = document.getElementById('auth-email').value.trim()
  const password = document.getElementById('auth-password').value
  const btn = document.getElementById('auth-btn')
  const errEl = document.getElementById('auth-error')

  btn.disabled = true
  btn.textContent = mode === 'login' ? 'Signing in…' : 'Creating account…'
  errEl.classList.add('hidden')

  try {
    if (mode === 'login') await login(email, password)
    else await register(email, password)
  } catch (e) {
    errEl.textContent = e.message
    errEl.classList.remove('hidden')
    btn.disabled = false
    btn.textContent = mode === 'login' ? 'Sign In' : 'Create Account'
  }
}

// ─── Page: Dashboard ───────────────────────────────────────────────────────────
function renderDashboardPage() {
  const statsText = formatStats()
  document.getElementById('app').innerHTML = `
    ${renderHeader()}
    <main class="max-w-5xl mx-auto px-4 py-8">
      <div class="flex items-center justify-between mb-6">
        <div>
          <h2 class="text-lg font-semibold text-white">Your Topics</h2>
          <p id="stats-counter" class="text-xs text-slate-500 mt-0.5">${statsText}</p>
        </div>
        <button onclick="toggleCreatePanel()"
          class="flex items-center gap-1.5 bg-indigo-600 hover:bg-indigo-500 text-white text-sm font-medium px-3.5 py-2 rounded-lg transition-colors">
          <svg class="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor">
            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2.5" d="M12 4v16m8-8H4"/>
          </svg>
          New Topic
        </button>
      </div>

      <!-- Create topic panel (hidden by default) -->
      <div id="create-panel" class="hidden mb-6 bg-slate-800/70 border border-slate-700 rounded-2xl p-5">
        <h3 class="text-sm font-semibold text-white mb-4">Create a new topic</h3>
        <form onsubmit="handleCreateTopic(event)">
          <div class="grid grid-cols-1 sm:grid-cols-2 gap-3">
            <div>
              <label class="block text-xs text-slate-400 mb-1.5">Topic name <span class="text-red-500">*</span></label>
              <input id="tp-name" type="text" required maxlength="120"
                placeholder="e.g. AI Agents"
                class="w-full bg-slate-900 border border-slate-700 rounded-lg px-3 py-2 text-sm text-white placeholder-slate-600 focus:outline-none focus:ring-2 focus:ring-indigo-500 transition" />
            </div>
            <div>
              <label class="block text-xs text-slate-400 mb-1.5">Keywords <span class="text-slate-600">(comma-separated)</span></label>
              <input id="tp-keywords" type="text"
                placeholder="e.g. ai, agents, llm, gpt"
                class="w-full bg-slate-900 border border-slate-700 rounded-lg px-3 py-2 text-sm text-white placeholder-slate-600 focus:outline-none focus:ring-2 focus:ring-indigo-500 transition" />
            </div>
          </div>
          <div id="create-err" class="hidden mt-2.5 text-xs text-red-400"></div>
          <div class="flex gap-2 mt-4">
            <button type="submit"
              class="bg-indigo-600 hover:bg-indigo-500 text-white text-sm font-medium px-4 py-2 rounded-lg transition-colors">
              Create
            </button>
            <button type="button" onclick="toggleCreatePanel()"
              class="text-slate-400 hover:text-white text-sm px-4 py-2 rounded-lg transition-colors">
              Cancel
            </button>
          </div>
        </form>
      </div>

      <!-- Topics grid -->
      <div id="topics-grid">${renderTopicsGrid()}</div>
    </main>
  `

  // Refresh stats silently in the background after the page renders.
  fetchStats().then(() => {
    const el = document.getElementById('stats-counter')
    if (el) el.textContent = formatStats()
  }).catch(() => {})
}

function renderTopicsGrid() {
  if (!state.topics.length) {
    return `
      <div class="text-center py-20 text-slate-600">
        <svg class="w-14 h-14 mx-auto mb-4 opacity-40" fill="none" viewBox="0 0 24 24" stroke="currentColor">
          <path stroke-linecap="round" stroke-linejoin="round" stroke-width="1.5"
            d="M9 19v-6a2 2 0 00-2-2H5a2 2 0 00-2 2v6a2 2 0 002 2h2a2 2 0 002-2zm0 0V9a2 2 0 012-2h2a2 2 0 012 2v10m-6 0a2 2 0 002 2h2a2 2 0 002-2m0 0V5a2 2 0 012-2h2a2 2 0 012 2v14a2 2 0 01-2 2h-2a2 2 0 01-2-2z"/>
        </svg>
        <p class="text-sm font-medium text-slate-500">No topics yet</p>
        <p class="text-xs text-slate-600 mt-1">Create your first topic to start monitoring</p>
      </div>
    `
  }

  return `
    <div class="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-4">
      ${state.topics.map(renderTopicCard).join('')}
    </div>
  `
}

function renderTopicCard(topic) {
  const chips = topic.keywords.slice(0, 5).map(k =>
    `<span class="inline-block bg-slate-700/80 text-slate-300 text-xs px-2 py-0.5 rounded-md">${escHtml(k)}</span>`
  ).join('')
  const overflow = topic.keywords.length > 5
    ? `<span class="text-xs text-slate-600">+${topic.keywords.length - 5}</span>` : ''

  const pausedPill = !topic.is_active
    ? `<span class="text-xs px-1.5 py-0.5 bg-amber-500/15 text-amber-400 border border-amber-500/25 rounded font-medium">paused</span>`
    : ''

  const dotColor = topic.is_active ? 'bg-green-400 pulse-dot' : 'bg-amber-500'

  // Pause icon (two vertical bars) when active; play icon (triangle) when paused.
  const pauseIcon = `<svg class="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor">
    <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2"
      d="M10 9v6m4-6v6m7-3a9 9 0 11-18 0 9 9 0 0118 0z"/>
  </svg>`
  const playIcon = `<svg class="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor">
    <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2"
      d="M14.752 11.168l-3.197-2.132A1 1 0 0010 9.87v4.263a1 1 0 001.555.832l3.197-2.132a1 1 0 000-1.664z"/>
    <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2"
      d="M21 12a9 9 0 11-18 0 9 9 0 0118 0z"/>
  </svg>`

  return `
    <div class="group bg-slate-800/70 border border-slate-700 hover:border-slate-600 rounded-2xl p-5 transition-colors flex flex-col gap-4 ${topic.is_active ? '' : 'opacity-60'}">
      <div class="flex items-start justify-between gap-2">
        <div class="flex items-center gap-2 flex-wrap min-w-0">
          <h3 class="font-semibold text-white text-sm leading-snug truncate">${escHtml(topic.name)}</h3>
          ${pausedPill}
        </div>
        <span class="shrink-0 mt-0.5 w-2 h-2 rounded-full ${dotColor}"></span>
      </div>
      <div class="flex flex-wrap gap-1.5 min-h-5">${chips}${overflow}</div>
      <div class="flex items-center gap-2 mt-auto">
        <button onclick="openTopic('${topic.id}')"
          class="flex-1 bg-indigo-600/20 hover:bg-indigo-600/35 border border-indigo-600/30 text-indigo-400 text-xs font-medium py-2 rounded-lg transition-colors">
          View Mentions
        </button>
        <button onclick="toggleTopicPause('${topic.id}')"
          title="${topic.is_active ? 'Pause ingestion' : 'Resume ingestion'}"
          class="p-2 text-slate-600 hover:text-amber-400 hover:bg-amber-500/10 rounded-lg transition-colors">
          ${topic.is_active ? pauseIcon : playIcon}
        </button>
        <button onclick="confirmDelete('${topic.id}','${escHtml(topic.name).replace(/'/g, "\\'")}')"
          title="Delete topic"
          class="p-2 text-slate-600 hover:text-red-400 hover:bg-red-500/10 rounded-lg transition-colors">
          <svg class="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor">
            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2"
              d="M19 7l-.867 12.142A2 2 0 0116.138 21H7.862a2 2 0 01-1.995-1.858L5 7m5 4v6m4-6v6m1-10V4a1 1 0 00-1-1h-4a1 1 0 00-1 1v3M4 7h16"/>
          </svg>
        </button>
      </div>
    </div>
  `
}

function toggleCreatePanel() {
  const panel = document.getElementById('create-panel')
  if (!panel) return
  const hidden = panel.classList.toggle('hidden')
  if (!hidden) document.getElementById('tp-name').focus()
}

async function handleCreateTopic(evt) {
  evt.preventDefault()
  const name = document.getElementById('tp-name').value.trim()
  const raw  = document.getElementById('tp-keywords').value
  const keywords = raw.split(',').map(k => k.trim()).filter(Boolean)
  const errEl = document.getElementById('create-err')

  try {
    const topic = await createTopic(name, keywords)
    if (topic) {
      toggleCreatePanel()
      document.getElementById('tp-name').value = ''
      document.getElementById('tp-keywords').value = ''
      document.getElementById('topics-grid').innerHTML = renderTopicsGrid()
      const el = document.getElementById('stats-counter')
      if (el) el.textContent = formatStats()
      showToast('Topic created', `Tracking "${topic.name}"`)
    }
  } catch (e) {
    errEl.textContent = e.message
    errEl.classList.remove('hidden')
  }
}

function confirmDelete(id, name) {
  if (!confirm(`Delete "${name}"?\n\nThis will also remove all associated mentions.`)) return
  deleteTopic(id)
    .then(() => {
      document.getElementById('topics-grid').innerHTML = renderTopicsGrid()
      const el = document.getElementById('stats-counter')
      if (el) el.textContent = formatStats()
      showToast('Topic deleted', `"${name}" was removed`)
    })
    .catch(e => showToast('Error', e.message))
}

function openTopic(id) {
  navigate('topic', { id })
}

// ─── Page: Topic Detail ────────────────────────────────────────────────────────
function renderTopicPage(topicId) {
  // Destroy any previous Chart.js instance before replacing the DOM.
  if (state.trendChart) {
    state.trendChart.destroy()
    state.trendChart = null
  }

  state.currentTopicId = topicId
  state.newMentionIds = new Set()

  const topic = state.topics.find(t => t.id === topicId)

  document.getElementById('app').innerHTML = `
    ${renderHeader()}
    <main class="max-w-3xl mx-auto px-4 py-8">
      <!-- Back + live indicator -->
      <div class="flex items-center justify-between mb-5">
        <button onclick="backToDashboard()"
          class="flex items-center gap-1.5 text-sm text-slate-400 hover:text-white transition-colors">
          <svg class="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor">
            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M15 19l-7-7 7-7"/>
          </svg>
          Dashboard
        </button>
        <div id="ws-indicator" class="flex items-center gap-1.5 text-xs">
          <span class="w-2 h-2 rounded-full bg-slate-600"></span>
          <span class="text-slate-500">Connecting…</span>
        </div>
      </div>

      <!-- Topic header -->
      <div class="mb-6">
        <div class="flex items-start justify-between gap-3">
          <div class="min-w-0">
            <h2 class="text-xl font-bold text-white">${topic ? escHtml(topic.name) : 'Topic'}</h2>
            ${topic && topic.keywords.length ? `
              <div class="flex flex-wrap gap-2 mt-2">
                ${topic.keywords.map(k =>
                  `<span class="bg-slate-800 border border-slate-700 text-slate-300 text-xs px-2.5 py-1 rounded-full">${escHtml(k)}</span>`
                ).join('')}
              </div>
            ` : ''}
          </div>
          ${topic ? `
            <button onclick="topicDetailPause('${topic.id}')"
              id="detail-pause-btn"
              title="${topic.is_active ? 'Pause ingestion' : 'Resume ingestion'}"
              class="shrink-0 flex items-center gap-1.5 px-3 py-1.5 text-xs font-medium rounded-lg border transition-colors
                ${topic.is_active
                  ? 'border-slate-700 text-slate-400 hover:text-amber-400 hover:border-amber-500/40 hover:bg-amber-500/10'
                  : 'border-amber-500/40 text-amber-400 bg-amber-500/10 hover:bg-amber-500/15'}">
              ${topic.is_active
                ? `<svg class="w-3.5 h-3.5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                     <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M10 9v6m4-6v6"/>
                   </svg>Pause`
                : `<svg class="w-3.5 h-3.5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                     <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2"
                       d="M14.752 11.168l-3.197-2.132A1 1 0 0010 9.87v4.263a1 1 0 001.555.832l3.197-2.132a1 1 0 000-1.664z"/>
                   </svg>Resume`}
            </button>
          ` : ''}
        </div>
      </div>

      <!-- Insights tabs: Trend / Summary -->
      <div class="mb-5 bg-slate-800/70 border border-slate-700 rounded-2xl overflow-hidden">
        <div class="flex border-b border-slate-700">
          <button onclick="switchInsightTab('trend')" id="tab-btn-trend"
            class="flex-1 py-2.5 text-xs font-semibold transition-colors text-indigo-400 border-b-2 border-indigo-500">
            Trend
          </button>
          <button onclick="switchInsightTab('summary')" id="tab-btn-summary"
            class="flex-1 py-2.5 text-xs font-semibold transition-colors text-slate-500 border-b-2 border-transparent hover:text-slate-300">
            Summary
          </button>
        </div>

        <div id="trend-section" class="p-4">
          <div class="h-20 flex items-center justify-center">
            <svg class="w-4 h-4 animate-spin text-slate-600" fill="none" viewBox="0 0 24 24">
              <circle class="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" stroke-width="4"/>
              <path class="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4z"/>
            </svg>
          </div>
        </div>

        <div id="digest-section" class="p-4 hidden">
          <div class="h-12 flex items-center justify-center">
            <svg class="w-4 h-4 animate-spin text-slate-600" fill="none" viewBox="0 0 24 24">
              <circle class="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" stroke-width="4"/>
              <path class="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4z"/>
            </svg>
          </div>
        </div>
      </div>

      <!-- Mentions feed -->
      <div id="mentions-feed">
        <div class="flex items-center justify-center gap-2.5 py-16 text-slate-500">
          <svg class="w-5 h-5 animate-spin" fill="none" viewBox="0 0 24 24">
            <circle class="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" stroke-width="4"/>
            <path class="opacity-75" fill="currentColor"
              d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"/>
          </svg>
          <span class="text-sm">Loading mentions…</span>
        </div>
      </div>
    </main>
  `

  // Fetch everything in parallel; each section updates independently.
  Promise.allSettled([
    fetchMentions(topicId, true).then(() => renderMentionsFeed()).catch(err => {
      const c = document.getElementById('mentions-feed')
      if (c) c.innerHTML = `
        <div class="text-center py-12">
          <p class="text-sm text-red-400">Failed to load mentions</p>
          <p class="text-xs text-slate-600 mt-1">${escHtml(err.message)}</p>
        </div>`
    }),
    fetchTrend(topicId).then(t => renderTrendChart(t)).catch(() => {
      const s = document.getElementById('trend-section')
      if (s) s.innerHTML = `<p class="text-xs text-slate-600 text-center py-3">Trend unavailable</p>`
    }),
    fetchDigest(topicId).then(d => renderDigestCard(d)).catch(() => {}),
  ]).finally(() => updateWsIndicator(state.wsConnected))

  connectWs()
  updateWsIndicator(state.wsConnected)
}

async function topicDetailPause(id) {
  const topic = state.topics.find(t => t.id === id)
  if (!topic) return
  const endpoint = topic.is_active ? `/topics/${id}/pause` : `/topics/${id}/resume`
  try {
    const updated = await api(endpoint, { method: 'PATCH' })
    if (!updated) return
    const idx = state.topics.findIndex(t => t.id === id)
    if (idx !== -1) state.topics[idx] = updated
    showToast(
      updated.is_active ? 'Topic resumed' : 'Topic paused',
      `"${updated.name}" ingestion ${updated.is_active ? 'resumed' : 'paused'}`
    )
    // Re-render only the button to reflect the new state.
    const btn = document.getElementById('detail-pause-btn')
    if (btn) {
      btn.title = updated.is_active ? 'Pause ingestion' : 'Resume ingestion'
      btn.className = `shrink-0 flex items-center gap-1.5 px-3 py-1.5 text-xs font-medium rounded-lg border transition-colors
        ${updated.is_active
          ? 'border-slate-700 text-slate-400 hover:text-amber-400 hover:border-amber-500/40 hover:bg-amber-500/10'
          : 'border-amber-500/40 text-amber-400 bg-amber-500/10 hover:bg-amber-500/15'}`
      btn.innerHTML = updated.is_active
        ? `<svg class="w-3.5 h-3.5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
             <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M10 9v6m4-6v6"/>
           </svg>Pause`
        : `<svg class="w-3.5 h-3.5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
             <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2"
               d="M14.752 11.168l-3.197-2.132A1 1 0 0010 9.87v4.263a1 1 0 001.555.832l3.197-2.132a1 1 0 000-1.664z"/>
           </svg>Resume`
    }
  } catch (e) {
    showToast('Error', e.message)
  }
}

// ─── Insight Tab Switcher ──────────────────────────────────────────────────────
function switchInsightTab(tab) {
  const trendBtn = document.getElementById('tab-btn-trend')
  const summaryBtn = document.getElementById('tab-btn-summary')
  const trendSection = document.getElementById('trend-section')
  const digestSection = document.getElementById('digest-section')
  if (!trendBtn) return
  const active = 'flex-1 py-2.5 text-xs font-semibold transition-colors text-indigo-400 border-b-2 border-indigo-500'
  const inactive = 'flex-1 py-2.5 text-xs font-semibold transition-colors text-slate-500 border-b-2 border-transparent hover:text-slate-300'
  if (tab === 'trend') {
    trendBtn.className = active; summaryBtn.className = inactive
    trendSection.classList.remove('hidden'); digestSection.classList.add('hidden')
  } else {
    summaryBtn.className = active; trendBtn.className = inactive
    digestSection.classList.remove('hidden'); trendSection.classList.add('hidden')
  }
}

// ─── Digest Card ───────────────────────────────────────────────────────────────
function renderDigestCard(digest) {
  const section = document.getElementById('digest-section')
  if (!section) return
  if (!digest) {
    section.innerHTML = `<p class="text-xs text-slate-600 text-center py-4">No summary yet — check back after a few ingestion cycles.</p>`
    return
  }

  const dist = digest.sentiment_distribution || {}
  const total = (dist.positive || 0) + (dist.negative || 0) + (dist.neutral || 0)
  const pct = n => total ? Math.round((n / total) * 100) : 0
  const posP = pct(dist.positive || 0)
  const neuP = pct(dist.neutral || 0)
  const negP = pct(dist.negative || 0)
  const entities = (digest.top_entities || []).slice(0, 8)

  section.innerHTML = `
    <div class="fade-in">
      <div class="flex items-center justify-between mb-3">
        <h3 class="text-sm font-semibold text-white">What people are saying</h3>
        <span class="text-xs text-slate-600">${digest.mention_count} mentions · ${timeAgo(digest.generated_at)}</span>
      </div>

      ${digest.summary
        ? `<p class="text-sm text-slate-300 leading-relaxed mb-4">${escHtml(digest.summary)}</p>`
        : '<p class="text-xs text-slate-500 mb-4">Summary generating…</p>'}

      ${total > 0 ? `
        <div class="mb-4">
          <p class="text-xs text-slate-500 mb-1.5">Sentiment distribution</p>
          <div class="flex h-1.5 rounded-full overflow-hidden gap-0.5">
            ${posP > 0 ? `<div class="bg-green-500 rounded-full" style="width:${posP}%"></div>` : ''}
            ${neuP > 0 ? `<div class="bg-slate-500 rounded-full" style="width:${neuP}%"></div>` : ''}
            ${negP > 0 ? `<div class="bg-red-500 rounded-full" style="width:${negP}%"></div>` : ''}
          </div>
          <div class="flex gap-4 mt-1.5 text-xs">
            <span class="text-green-400">${posP}% positive</span>
            <span class="text-slate-500">${neuP}% neutral</span>
            <span class="text-red-400">${negP}% negative</span>
          </div>
        </div>
      ` : ''}

      ${entities.length ? `
        <div>
          <p class="text-xs text-slate-500 mb-1.5">Top entities</p>
          <div class="flex flex-wrap gap-1.5">
            ${entities.map(e =>
              `<span class="bg-indigo-500/15 text-indigo-400 border border-indigo-500/25 text-xs px-2 py-0.5 rounded">#${escHtml(e)}</span>`
            ).join('')}
          </div>
        </div>
      ` : ''}
    </div>
  `
}

// ─── Trend Chart ───────────────────────────────────────────────────────────────
function renderTrendChart(trend) {
  const section = document.getElementById('trend-section')
  if (!section) return

  if (!trend || !trend.total === undefined) {
    section.innerHTML = `<p class="text-xs text-slate-600 text-center py-4">Not enough data yet — check back after a few ingestion cycles</p>`
    return
  }

  const sentLabel = trend.avg_sentiment == null ? 'no data'
    : trend.avg_sentiment > 0.2 ? 'positive'
    : trend.avg_sentiment < -0.2 ? 'negative' : 'neutral'
  const sentColor = sentLabel === 'positive' ? 'text-green-400'
    : sentLabel === 'negative' ? 'text-red-400' : 'text-slate-400'
  const sentScore = trend.avg_sentiment != null
    ? `${trend.avg_sentiment >= 0 ? '+' : ''}${trend.avg_sentiment.toFixed(2)}` : '—'

  const sources = trend.sources || {}
  const srcTotal = Object.values(sources).reduce((a, b) => a + b, 0)
  const srcBars = Object.entries(sources)
    .sort((a, b) => b[1] - a[1])
    .map(([name, count]) => {
      const pct = srcTotal ? Math.round((count / srcTotal) * 100) : 0
      const color = name === 'hackernews' ? 'bg-orange-500' : 'bg-blue-500'
      return `
        <div class="flex items-center gap-2">
          <span class="text-xs text-slate-500 w-20 shrink-0 capitalize">${name === 'hackernews' ? 'Hacker News' : name}</span>
          <div class="flex-1 h-1.5 bg-slate-700 rounded-full overflow-hidden">
            <div class="${color} h-full rounded-full" style="width:${pct}%"></div>
          </div>
          <span class="text-xs text-slate-500 w-8 text-right">${count}</span>
        </div>`
    }).join('')

  section.innerHTML = `
    <div class="flex gap-3 mb-4">
      <div class="flex-1 bg-slate-700/40 rounded-xl p-3 text-center">
        <p class="text-lg font-bold text-white">${trend.total}</p>
        <p class="text-xs text-slate-500 mt-0.5">mentions</p>
      </div>
      <div class="flex-1 bg-slate-700/40 rounded-xl p-3 text-center">
        <p class="text-lg font-bold ${sentColor}">${sentScore}</p>
        <p class="text-xs text-slate-500 mt-0.5">${sentLabel}</p>
      </div>
      <div class="flex-1 bg-slate-700/40 rounded-xl p-3 text-center">
        <p class="text-lg font-bold text-white">${Object.keys(sources).length}</p>
        <p class="text-xs text-slate-500 mt-0.5">sources</p>
      </div>
    </div>

    ${trend.points.length > 0 ? `
      <div class="mb-3">
        <p class="text-xs text-slate-500 mb-1.5">Volume &amp; sentiment over time</p>
        <div class="h-24"><canvas id="trend-chart"></canvas></div>
      </div>
    ` : '<p class="text-xs text-slate-600 text-center py-2 mb-3">Chart available after more data accumulates</p>'}

    ${srcBars ? `
      <div>
        <p class="text-xs text-slate-500 mb-2">Sources</p>
        <div class="flex flex-col gap-1.5">${srcBars}</div>
      </div>
    ` : ''}
  `

  if (typeof Chart === 'undefined' || !trend.points.length) return

  const canvas = document.getElementById('trend-chart')
  if (!canvas) return
  const labels = trend.points.map(p => {
    const d = new Date(p.bucket)
    return `${d.getHours().toString().padStart(2, '0')}:00`
  })
  const counts = trend.points.map(p => p.count)
  const scores = trend.points.map(p => p.avg_score != null ? parseFloat(p.avg_score.toFixed(2)) : null)

  state.trendChart = new Chart(canvas, {
    data: {
      labels,
      datasets: [
        {
          type: 'bar',
          label: 'Mentions',
          data: counts,
          backgroundColor: 'rgba(99,102,241,0.25)',
          borderColor: 'rgba(99,102,241,0.5)',
          borderWidth: 1,
          borderRadius: 3,
          yAxisID: 'yVol',
        },
        {
          type: 'line',
          label: 'Sentiment',
          data: scores,
          borderColor: '#34d399',
          backgroundColor: 'transparent',
          borderWidth: 2,
          tension: 0.4,
          pointBackgroundColor: '#34d399',
          pointRadius: 3,
          pointHoverRadius: 4,
          spanGaps: true,
          yAxisID: 'ySent',
        }
      ]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      animation: { duration: 300 },
      plugins: {
        legend: { display: false },
        tooltip: {
          backgroundColor: '#1e293b',
          borderColor: '#334155',
          borderWidth: 1,
          titleColor: '#94a3b8',
          bodyColor: '#e2e8f0',
        }
      },
      scales: {
        x: { grid: { color: '#1e293b' }, ticks: { color: '#475569', font: { size: 10 }, maxTicksLimit: 8 } },
        yVol: {
          position: 'left',
          grid: { color: '#1e293b' },
          ticks: { color: '#475569', font: { size: 10 }, maxTicksLimit: 4 },
          beginAtZero: true,
        },
        ySent: {
          position: 'right',
          min: -1, max: 1,
          grid: { drawOnChartArea: false },
          ticks: { color: '#34d399', font: { size: 10 }, callback: v => v.toFixed(1) },
        }
      }
    }
  })
}

// ─── Mentions Feed ─────────────────────────────────────────────────────────────
function renderMentionsFeed() {
  const container = document.getElementById('mentions-feed')
  if (!container) return

  if (!state.mentions.length) {
    container.innerHTML = `
      <div class="text-center py-16">
        <p class="text-sm text-slate-500">No mentions yet.</p>
        <p class="text-xs text-slate-600 mt-1">New mentions will appear here once the analyzer processes them.</p>
      </div>
    `
    return
  }

  const hasMore = state.mentions.length < state.mentionsTotal
  container.innerHTML = `
    <div class="space-y-3">
      ${state.mentions.map(renderMentionCard).join('')}
    </div>
    ${hasMore ? `
      <div class="text-center mt-6">
        <button onclick="loadMoreMentions()"
          class="text-sm text-slate-400 hover:text-white border border-slate-700 hover:border-slate-600 px-5 py-2 rounded-xl transition-colors">
          Load more
          <span class="text-slate-600 ml-1">(${state.mentionsTotal - state.mentions.length} left)</span>
        </button>
      </div>
    ` : ''}
  `
}

function renderMentionCard(m) {
  const isNew = state.newMentionIds.has(m.id)
  const isPending = !m.analyzed_at

  const newBadge = isNew
    ? '<span class="inline-flex items-center px-1.5 py-0.5 rounded text-xs bg-yellow-500/15 text-yellow-400 border border-yellow-500/25 font-semibold">NEW</span>'
    : ''
  const pendingBadge = isPending
    ? '<span class="inline-flex items-center gap-1 px-1.5 py-0.5 rounded text-xs bg-slate-700 text-slate-500 border border-slate-600">analyzing…</span>'
    : ''

  const titleEl = m.url
    ? `<a href="${escHtml(m.url)}" target="_blank" rel="noopener noreferrer"
         class="hover:text-indigo-400 transition-colors">${escHtml(m.title || 'Untitled')}</a>`
    : escHtml(m.title || 'Untitled')

  const entities = m.entities && m.entities.length
    ? `<div class="flex flex-wrap gap-1 mt-2.5">
         ${m.entities.slice(0, 8).map(e =>
           `<span class="bg-slate-700/60 text-slate-400 text-xs px-1.5 py-0.5 rounded">#${escHtml(e)}</span>`
         ).join('')}
       </div>`
    : ''

  return `
    <div class="bg-slate-800/70 border ${isNew ? 'border-indigo-500/40 shadow-indigo-900/20 shadow-lg' : 'border-slate-700'} rounded-2xl p-4 ${isNew ? 'fade-in' : ''}">
      <div class="flex flex-wrap items-center gap-2 mb-2.5">
        ${newBadge}
        ${sourceChip(m.source)}
        ${isPending ? pendingBadge : sentimentChip(m.sentiment_label, m.sentiment_score)}
        <span class="ml-auto text-xs text-slate-600">${timeAgo(m.ingested_at)}</span>
      </div>
      <h4 class="text-sm font-semibold text-white leading-snug">${titleEl}</h4>
      ${m.summary
        ? `<p class="text-xs text-slate-400 mt-1.5 leading-relaxed">${escHtml(m.summary)}</p>`
        : ''}
      ${entities}
      ${m.author
        ? `<p class="text-xs text-slate-600 mt-2">by ${escHtml(m.author)}</p>`
        : ''}
    </div>
  `
}

async function loadMoreMentions() {
  await fetchMentions(state.currentTopicId)
  renderMentionsFeed()
}

function backToDashboard() {
  if (state.trendChart) {
    state.trendChart.destroy()
    state.trendChart = null
  }
  state.currentTopicId = null
  navigate('dashboard')
}

// ─── Shared Header ─────────────────────────────────────────────────────────────
function renderHeader() {
  return `
    <header class="sticky top-0 z-40 bg-slate-900/80 backdrop-blur-sm border-b border-slate-800">
      <div class="max-w-5xl mx-auto px-4 h-14 flex items-center justify-between">
        <div class="flex items-center gap-2.5">
          <div class="w-7 h-7 rounded-lg bg-indigo-600 flex items-center justify-center">
            <svg class="w-4 h-4 text-white" fill="none" viewBox="0 0 24 24" stroke="currentColor">
              <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2.5" d="M13 10V3L4 14h7v7l9-11h-7z"/>
            </svg>
          </div>
          <span class="font-bold text-sm text-white tracking-tight">PulseStream</span>
        </div>
        <div class="flex items-center gap-4">
          ${state.user ? `<span class="text-xs text-slate-500 hidden sm:block">${escHtml(state.user.email)}</span>` : ''}
          <button onclick="logout()"
            class="text-xs text-slate-400 hover:text-white transition-colors">
            Sign out
          </button>
        </div>
      </div>
    </header>
  `
}

// ─── Init ──────────────────────────────────────────────────────────────────────
async function init() {
  if (!state.token) {
    navigate('login')
    return
  }
  try {
    await Promise.all([loadUser(), fetchTopics(), fetchStats(), fetchConfig()])
    if (!state.token) return  // 401 inside api() already called logout()
    navigate('dashboard')
    startHeartbeat()
    connectWs()
  } catch {
    // Don't call logout() on transient errors — the token stays valid.
    // If it was a real 401, api() already called logout() above.
    navigate('login')
  }
}

init()
