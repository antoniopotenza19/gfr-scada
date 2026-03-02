// Minimal JS to handle login, fetch and open websocket
async function postJSON(url, data) {
  const res = await fetch(url, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(data)
  })
  return res.json()
}

document.addEventListener('DOMContentLoaded', () => {
  const loginForm = document.getElementById('login')
  if (loginForm) {
    loginForm.addEventListener('submit', async (e) => {
      e.preventDefault()
      const fd = new FormData(loginForm)
      const username = fd.get('username')
      const password = fd.get('password')
      const resp = await postJSON('/api/auth/login', { username, password })
      if (resp.access_token) {
        sessionStorage.setItem('access_token', resp.access_token)
        window.location.href = '/static/dashboard.html'
      } else {
        alert('Login failed')
      }
    })
  }

  if (window.location.pathname.endsWith('realtime.html')) {
    const messages = document.getElementById('messages')
    const ws = new WebSocket((location.protocol === 'https:' ? 'wss' : 'ws') + '://' + location.host + '/ws/realtime')
    ws.onmessage = (ev) => {
      const p = document.createElement('div')
      p.textContent = ev.data
      messages.appendChild(p)
    }
  }

  if (window.location.pathname.endsWith('dashboard.html')) {
    const exportBtn = document.getElementById('export')
    exportBtn && exportBtn.addEventListener('click', async () => {
      // simple CSV export of KPIs (placeholder)
      const data = [['kpi','value'], ['uptime', '99.9']]
      const csv = data.map(r => r.join(',')).join('\n')
      const blob = new Blob([csv], { type: 'text/csv' })
      const url = URL.createObjectURL(blob)
      const a = document.createElement('a')
      a.href = url; a.download='kpis.csv'
      a.click()
    })
  }
})
