import { useEffect } from 'react'
import { Navigate, Route, Routes, useLocation, useParams } from 'react-router-dom'
import Login from './pages/Login'
import Dashboard from './pages/Dashboard'
import Scada from './pages/Scada'
import Alarms from './pages/Alarms'
import Sites from './pages/Sites'
import SiteDetail from './pages/SiteDetail'
import NotAuthorized from './pages/NotAuthorized'
import DevTools from './pages/DevTools'
import SalaChartsPage from './pages/SalaChartsPage'
import { canViewDevFeatures, canViewSite, defaultPathForUser, getAuthUserFromSessionToken } from './utils/auth'
import { getSelectedSiteId } from './utils/siteSelection'
import { isSiteId } from './constants/sites'

function RequireAuth({ children }: { children: JSX.Element }) {
  const token = sessionStorage.getItem('gfr_token')
  if (!token) return <Navigate to="/login" replace />
  return children
}

function RequireSiteAccess({ children }: { children: JSX.Element }) {
  const { siteId } = useParams()
  const user = getAuthUserFromSessionToken()
  if (!siteId || !isSiteId(siteId)) return <Navigate to="/sites" replace />
  if (!canViewSite(user, siteId)) return <Navigate to="/403" replace />
  return children
}

function RequireDev({ children }: { children: JSX.Element }) {
  const user = getAuthUserFromSessionToken()
  if (!canViewDevFeatures(user)) return <Navigate to="/403" replace />
  return children
}

function RequireSelectedSite({ children }: { children: JSX.Element }) {
  const user = getAuthUserFromSessionToken()
  if (user.allowedSiteIds.length <= 1) return children
  const selectedSiteId = getSelectedSiteId()
  if (!selectedSiteId || !canViewSite(user, selectedSiteId)) return <Navigate to="/sites" replace />
  return children
}

function ScrollToTopOnRouteChange() {
  const location = useLocation()

  useEffect(() => {
    const scrollToTop = () => {
      window.scrollTo({ top: 0, left: 0, behavior: 'auto' })
      document.documentElement.scrollTop = 0
      document.body.scrollTop = 0
      document.querySelector<HTMLElement>('.app-shell-main')?.scrollTo({ top: 0, left: 0, behavior: 'auto' })
      document.querySelector<HTMLElement>('.app-shell-content')?.scrollTo({ top: 0, left: 0, behavior: 'auto' })
    }

    scrollToTop()
    const frameId = window.requestAnimationFrame(scrollToTop)
    return () => window.cancelAnimationFrame(frameId)
  }, [location.pathname, location.search, location.hash])

  return null
}

export default function App() {
  const token = sessionStorage.getItem('gfr_token')
  const authUser = getAuthUserFromSessionToken()
  const defaultAuthedPath = defaultPathForUser(authUser)

  return (
    <>
      <ScrollToTopOnRouteChange />
      <Routes>
        <Route path="/login" element={<Login />} />
        <Route
          path="/"
          element={token ? <Navigate to={defaultAuthedPath} /> : <Navigate to="/login" />}
        />
        <Route path="/403" element={<RequireAuth><NotAuthorized /></RequireAuth>} />
        <Route path="/sites" element={<RequireAuth><Sites /></RequireAuth>} />
        <Route path="/sites/:siteId" element={<RequireAuth><RequireSiteAccess><SiteDetail /></RequireSiteAccess></RequireAuth>} />
        <Route path="/dashboard" element={<RequireAuth><RequireSelectedSite><Dashboard /></RequireSelectedSite></RequireAuth>} />
        <Route path="/scada/:plant" element={<RequireAuth><RequireSelectedSite><Scada /></RequireSelectedSite></RequireAuth>} />
        <Route path="/sale/:saleId/grafici" element={<RequireAuth><RequireSelectedSite><SalaChartsPage /></RequireSelectedSite></RequireAuth>} />
        <Route path="/alarms" element={<RequireAuth><RequireSelectedSite><Alarms /></RequireSelectedSite></RequireAuth>} />
        <Route path="/dev" element={<RequireAuth><RequireSelectedSite><RequireDev><DevTools /></RequireDev></RequireSelectedSite></RequireAuth>} />
        <Route path="*" element={<Navigate to={token ? defaultAuthedPath : '/login'} replace />} />
      </Routes>
    </>
  )
}
