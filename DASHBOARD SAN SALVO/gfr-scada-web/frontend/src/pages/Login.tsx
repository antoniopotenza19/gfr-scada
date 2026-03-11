import React, { useState } from 'react'
import { useNavigate } from 'react-router-dom'
import { defaultPathForUser, getAuthUserFromSessionToken } from '../utils/auth'
import { clearSelectedSiteId } from '../utils/siteSelection'

export default function Login() {
  const [user, setUser] = useState('')
  const [pass, setPass] = useState('')
  const navigate = useNavigate()

  const submit = async (e: React.FormEvent) => {
    e.preventDefault()
    const baseUrl = (import.meta.env.VITE_API_BASE_URL as string | undefined) || 'http://127.0.0.1:8000'

    const resp = await fetch(`${baseUrl}/api/auth/login`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ username: user, password: pass }),
      credentials: 'include',
    })

    if (!resp.ok) {
      alert('Invalid credentials')
      return
    }

    const data = await resp.json()
    if (data.access_token) {
      clearSelectedSiteId()
      sessionStorage.setItem('gfr_token', data.access_token)
      navigate(defaultPathForUser(getAuthUserFromSessionToken()))
      return
    }

    alert('Login failed')
  }

  return (
    <div className="min-h-screen flex items-center justify-center bg-slate-50">
      <div className="w-full max-w-md bg-white border border-slate-200 p-8 rounded-lg shadow-sm">
        <div className="text-center mb-6">
          <div className="h-12 w-12 rounded-md bg-slate-200 mx-auto mb-2" />
          <h1 className="text-2xl font-semibold">GFR Engineering</h1>
          <p className="text-sm text-slate-500">Login to your dashboard</p>
        </div>
        <form onSubmit={submit} className="space-y-4">
          <div>
            <label className="block text-sm text-slate-700">Username</label>
            <input className="mt-1 w-full border border-slate-200 rounded px-3 py-2" value={user} onChange={e => setUser(e.target.value)} />
          </div>
          <div>
            <label className="block text-sm text-slate-700">Password</label>
            <input type="password" className="mt-1 w-full border border-slate-200 rounded px-3 py-2" value={pass} onChange={e => setPass(e.target.value)} />
          </div>
          <div className="flex items-center justify-between">
            <label className="text-sm">
              <input type="checkbox" className="mr-2" /> Remember me
            </label>
            <button className="px-4 py-2 bg-slate-800 text-white rounded">Sign in</button>
          </div>
        </form>
      </div>
    </div>
  )
}

