import axios from 'axios'

function normalizeApiBaseUrl(raw: string): string {
  try {
    const parsed = new URL(raw)
    if (import.meta.env.DEV && (parsed.hostname === 'localhost' || parsed.hostname === '::1' || parsed.hostname === '[::1]')) {
      parsed.hostname = '127.0.0.1'
    }
    return parsed.toString().replace(/\/$/, '')
  } catch {
    return raw
  }
}

const envBase = (import.meta.env.VITE_API_BASE_URL as string | undefined)?.trim()
const base = normalizeApiBaseUrl(envBase || 'http://127.0.0.1:8000')

const api = axios.create({ baseURL: base, withCredentials: true })

api.interceptors.request.use((config) => {
  const token = sessionStorage.getItem('gfr_token')
  if (token && config.headers) config.headers['Authorization'] = `Bearer ${token}`
  return config
})

api.interceptors.response.use(
  (response) => response,
  async (error) => {
    const originalRequest = error.config || {}
    if (error?.response?.status === 401 && !originalRequest._retry) {
      originalRequest._retry = true
      try {
        const resp = await axios.post(`${base}/api/auth/refresh`, {}, { withCredentials: true })
        const newToken = resp.data?.access_token
        if (newToken) {
          sessionStorage.setItem('gfr_token', newToken)
          originalRequest.headers = originalRequest.headers || {}
          originalRequest.headers['Authorization'] = `Bearer ${newToken}`
          return api(originalRequest)
        }
      } catch (_e) {
        sessionStorage.removeItem('gfr_token')
      }
    }
    return Promise.reject(error)
  }
)

export default api
