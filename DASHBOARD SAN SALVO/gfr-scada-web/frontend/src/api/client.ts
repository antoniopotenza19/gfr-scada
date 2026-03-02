import axios from 'axios'

const base = import.meta.env.VITE_API_BASE_URL || 'http://localhost:8000'

const api = axios.create({ baseURL: base })

api.interceptors.request.use((config)=>{
  const token = sessionStorage.getItem('gfr_token')
  if (token && config.headers) config.headers['Authorization'] = `Bearer ${token}`
  return config
})

export default api
