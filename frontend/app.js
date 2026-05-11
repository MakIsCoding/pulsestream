'use strict'

// ─── Config ────────────────────────────────────────────────────────────────────
// WebSocket lives on a separate port. API calls are relative (same origin as web).
const WS_PORT = 8001

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
}

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
  await loadUser()
  await fetchTopics()
  navigate('dashboard')
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
  await loadUser()
  await fetchTopics()
  navigate('dashboard')
  connectWs()
}

function logout() {
  state.token = null
  state.user = null
  state.topics = []
  state.currentTopicId = null
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
  if (topic) state.topics.unshift(topic)
  return topic
}

async function deleteTopic(id) {
  await api(`/topics/${id}`, { method: 'DELETE' })
  state.topics = state.topics.filter(t => t.id !== id)
}

// ─── Mentions ──────────────────────────────────────────────────────────────────
async function fetchMentions(topicId, reset = false) {
  // Use a local offset so state isn't mutated until the fetch succeeds.
  // This prevents the "disappears" bug where navigating back and forward
  // clears state.mentions before the new data arrives.
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

// ─── WebSocket ─────────────────────────────────────────────────────────────────
function connectWs() {
  if (state.ws || !state.token) return
  const url = `ws://${window.location.hostname}:${WS_PORT}/ws?token=${state.token}`
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
  // Update topic-detail feed if the user is currently viewing this topic.
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
  document.getElementById('app').innerHTML = `
    ${renderHeader()}
    <main class="max-w-5xl mx-auto px-4 py-8">
      <div class="flex items-center justify-between mb-6">
        <h2 class="text-lg font-semibold text-white">Your Topics</h2>
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

  return `
    <div class="group bg-slate-800/70 border border-slate-700 hover:border-slate-600 rounded-2xl p-5 transition-colors flex flex-col gap-4">
      <div class="flex items-start justify-between gap-2">
        <h3 class="font-semibold text-white text-sm leading-snug">${escHtml(topic.name)}</h3>
        <span class="shrink-0 mt-0.5 w-2 h-2 rounded-full ${topic.is_active ? 'bg-green-400 pulse-dot' : 'bg-slate-600'}"></span>
      </div>
      <div class="flex flex-wrap gap-1.5 min-h-5">${chips}${overflow}</div>
      <div class="flex items-center gap-2 mt-auto">
        <button onclick="openTopic('${topic.id}')"
          class="flex-1 bg-indigo-600/20 hover:bg-indigo-600/35 border border-indigo-600/30 text-indigo-400 text-xs font-medium py-2 rounded-lg transition-colors">
          View Mentions
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
      showToast('Topic deleted', `"${name}" was removed`)
    })
    .catch(e => showToast('Error', e.message))
}

function openTopic(id) {
  navigate('topic', { id })
}

// ─── Page: Topic Detail ────────────────────────────────────────────────────────
function renderTopicPage(topicId) {
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
      <div class="mb-7">
        <h2 class="text-xl font-bold text-white">${topic ? escHtml(topic.name) : 'Topic'}</h2>
        ${topic && topic.keywords.length ? `
          <div class="flex flex-wrap gap-2 mt-2.5">
            ${topic.keywords.map(k =>
              `<span class="bg-slate-800 border border-slate-700 text-slate-300 text-xs px-2.5 py-1 rounded-full">${escHtml(k)}</span>`
            ).join('')}
          </div>
        ` : ''}
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

  // Fetch then render, and (re)connect WS
  fetchMentions(topicId, true)
    .then(() => renderMentionsFeed())
    .catch(err => {
      const container = document.getElementById('mentions-feed')
      if (container) container.innerHTML = `
        <div class="text-center py-12">
          <p class="text-sm text-red-400">Failed to load mentions</p>
          <p class="text-xs text-slate-600 mt-1">${escHtml(err.message)}</p>
        </div>`
    })
    .finally(() => updateWsIndicator(state.wsConnected))
  connectWs()
  updateWsIndicator(state.wsConnected)
}

function renderMentionsFeed() {
  const container = document.getElementById('mentions-feed')
  if (!container) return

  if (!state.mentions.length) {
    container.innerHTML = `
      <div class="text-center py-16">
        <p class="text-sm text-slate-500">No analyzed mentions yet.</p>
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
    await loadUser()
    await fetchTopics()
    navigate('dashboard')
    connectWs()
  } catch {
    logout()
  }
}

init()
