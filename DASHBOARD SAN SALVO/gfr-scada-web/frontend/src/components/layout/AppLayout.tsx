import { useEffect, type ReactNode } from 'react'
import { Link, NavLink, useLocation, useNavigate } from 'react-router-dom'
import { usePlants } from '../../hooks/usePlants'
import { getLastSelectedSala, setLastSelectedSala } from '../../utils/saleNavigation'
import { canViewDevFeatures, getAuthUserFromSessionToken } from '../../utils/auth'
import { clearSelectedSiteId } from '../../utils/siteSelection'
import './app-layout.css'

interface AppLayoutProps {
  title: string
  subtitle: string
  plant: string
  onPlantChange: (plant: string) => void
  selectorOptions?: string[]
  selectorPlaceholder?: string
  scadaPlant?: string
  chartsPlant?: string
  navigationLocked?: boolean
  children: ReactNode
}

interface SidebarItem {
  label: string
  to?: string
  state?: unknown
  icon: 'plants' | 'dashboard' | 'scada' | 'charts' | 'alarms' | 'dev'
}
type SidebarIcon = SidebarItem['icon']

function NavGlyph({ kind }: { kind: SidebarIcon }) {
  if (kind === 'plants') {
    return (
      <svg viewBox="0 0 24 24" aria-hidden="true">
        <path d="M4 20h16M6 20V7l6-3 6 3v13M10 10h4M10 14h4" fill="none" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" />
      </svg>
    )
  }
  if (kind === 'dashboard') {
    return (
      <svg viewBox="0 0 24 24" aria-hidden="true">
        <path d="M4 12h7V4H4v8Zm9 8h7v-6h-7v6Zm0-10h7V4h-7v6Zm-9 10h7v-4H4v4Z" fill="none" stroke="currentColor" strokeWidth="1.8" strokeLinejoin="round" />
      </svg>
    )
  }
  if (kind === 'scada') {
    return (
      <svg viewBox="0 0 24 24" aria-hidden="true">
        <path d="M4 6h16v10H4zM9 20h6M12 16v4" fill="none" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" />
      </svg>
    )
  }
  if (kind === 'alarms') {
    return (
      <svg viewBox="0 0 24 24" aria-hidden="true">
        <path d="M12 4 3.5 19h17L12 4Zm0 5v5m0 3h.01" fill="none" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round" />
      </svg>
    )
  }
  if (kind === 'charts') {
    return (
      <svg viewBox="0 0 24 24" aria-hidden="true">
        <path d="M4 18h16M6 15l4-4 3 2 5-6" fill="none" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round" />
        <circle cx="6" cy="15" r="1.2" fill="currentColor" />
        <circle cx="10" cy="11" r="1.2" fill="currentColor" />
        <circle cx="13" cy="13" r="1.2" fill="currentColor" />
        <circle cx="18" cy="7" r="1.2" fill="currentColor" />
      </svg>
    )
  }
  return (
    <svg viewBox="0 0 24 24" aria-hidden="true">
      <path d="M7 8h10M7 12h6m-6 4h10M5 4h14a1 1 0 0 1 1 1v14a1 1 0 0 1-1 1H5a1 1 0 0 1-1-1V5a1 1 0 0 1 1-1Z" fill="none" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round" />
    </svg>
  )
}

function SidebarLink({ item, disabled = false }: { item: SidebarItem; disabled?: boolean }) {
  if (!item.to || disabled) {
    return (
      <span className="app-shell-nav-item is-disabled">
        <span className="app-shell-nav-icon" aria-hidden="true"><NavGlyph kind={item.icon} /></span>
        <span>{item.label}</span>
      </span>
    )
  }

  return (
    <NavLink
      to={item.to}
      state={item.state}
      className={({ isActive }) => `app-shell-nav-item${isActive ? ' is-active' : ''}`}
    >
      <span className="app-shell-nav-icon" aria-hidden="true"><NavGlyph kind={item.icon} /></span>
      <span>{item.label}</span>
    </NavLink>
  )
}

function HeaderGlyph({ locationPath }: { locationPath: string }) {
  if (locationPath.startsWith('/sites')) return <NavGlyph kind="plants" />
  if (locationPath.startsWith('/scada/')) return <NavGlyph kind="scada" />
  if (locationPath.startsWith('/sale/') && locationPath.endsWith('/grafici')) return <NavGlyph kind="charts" />
  if (locationPath.startsWith('/alarms')) return <NavGlyph kind="alarms" />
  if (locationPath.startsWith('/dev')) return <NavGlyph kind="dev" />
  return <NavGlyph kind="dashboard" />
}

export default function AppLayout({
  title,
  subtitle,
  plant,
  onPlantChange,
  selectorOptions,
  selectorPlaceholder = 'Select plant',
  scadaPlant,
  chartsPlant,
  navigationLocked = false,
  children,
}: AppLayoutProps) {
  const navigate = useNavigate()
  const location = useLocation()
  const { data: plants, isLoading } = usePlants()
  const user = getAuthUserFromSessionToken()
  const options = selectorOptions ?? plants ?? []
  const optionsLoading = selectorOptions ? false : isLoading
  const showPlantSelector = options.length > 1
  const showSitesMenu = user.allowedSiteIds.length > 1

  useEffect(() => {
    if (plant && options.length > 0 && !options.includes(plant)) {
      onPlantChange('')
    }
  }, [options, plant, onPlantChange])

  useEffect(() => {
    if (chartsPlant) {
      setLastSelectedSala(chartsPlant)
    }
  }, [chartsPlant])

  const handleLogout = () => {
    sessionStorage.removeItem('gfr_token')
    clearSelectedSiteId()
    navigate('/login')
  }

  const isScadaRoute = location.pathname.startsWith('/scada/')
  const isChartsRoute = location.pathname.startsWith('/sale/') && location.pathname.endsWith('/grafici')
  const scadaName = scadaPlant || getLastSelectedSala()
  const chartsName = chartsPlant || getLastSelectedSala()
  const scadaTarget = scadaName ? `/scada/${encodeURIComponent(scadaName)}` : undefined
  const chartsTarget = chartsName ? `/sale/${encodeURIComponent(chartsName)}/grafici` : undefined

  const items: SidebarItem[] = [
    ...(showSitesMenu ? ([{ label: 'Impianti', to: '/sites', icon: 'plants' as SidebarIcon }]) : []),
    { label: 'Dashboard', to: '/dashboard', state: { resetDashboard: true, scrollToTop: true }, icon: 'dashboard' as SidebarIcon },
    { label: 'SCADA', to: isScadaRoute ? location.pathname : scadaTarget, icon: 'scada' as SidebarIcon },
    { label: 'Grafici', to: isChartsRoute ? location.pathname : chartsTarget, state: { scrollToTop: true }, icon: 'charts' as SidebarIcon },
    { label: 'Allarmi', to: '/alarms', icon: 'alarms' as SidebarIcon },
  ]
  if (canViewDevFeatures(user)) items.push({ label: 'Dev', to: '/dev', icon: 'dev' as SidebarIcon })

  return (
    <div className="app-shell">
      <aside className="app-shell-sidebar">
        <div className="app-shell-brand">
          {navigationLocked ? (
            <div className="app-shell-brand-link is-disabled">
              <div className="app-shell-brand-title">GFR Engineering</div>
              <div className="app-shell-brand-subtitle">Energy Saving</div>
            </div>
          ) : (
            <Link to="/dashboard" state={{ resetDashboard: true, scrollToTop: true }} className="app-shell-brand-link">
              <div className="app-shell-brand-title">GFR Engineering</div>
              <div className="app-shell-brand-subtitle">Energy Saving</div>
            </Link>
          )}
        </div>

        <nav className="app-shell-nav">
          {items.map((item) => (
            <SidebarLink key={item.label} item={item} disabled={navigationLocked && item.icon !== 'plants'} />
          ))}
        </nav>

        <div className="app-shell-user-wrap">
          <div className="app-shell-user-card">
            <div className="app-shell-user-identity">
              <span className="app-shell-user-icon" aria-hidden="true">
                <svg viewBox="0 0 24 24">
                  <path d="M12 12a4 4 0 1 0-4-4 4 4 0 0 0 4 4Zm0 2c-3.3 0-6 1.7-6 3.8V20h12v-2.2c0-2.1-2.7-3.8-6-3.8Z" fill="none" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round" />
                </svg>
              </span>
              <span>{user.role}</span>
            </div>
          </div>
          <button
            type="button"
            onClick={handleLogout}
            className="app-shell-logout app-shell-logout-sidebar"
          >
            Logout
          </button>
        </div>
      </aside>

      <div className="app-shell-main">
        <div id="app-page-top-anchor" aria-hidden="true" />
        <header className="app-shell-header">
          <div className="app-shell-header-inner">
            <div className="app-shell-headings">
              <h1 id="app-page-title-anchor" className="app-shell-title">
                <span className="app-shell-title-icon" aria-hidden="true"><HeaderGlyph locationPath={location.pathname} /></span>
                <span>{title}</span>
              </h1>
            </div>

            <div className="app-shell-controls-group">
            <div className="app-shell-controls">
              {showPlantSelector ? (
                <label className="app-shell-control app-shell-control-select">
                  <span className="app-shell-control-icon" aria-hidden="true">
                    <svg viewBox="0 0 24 24">
                      <path d="M4 20h16M6 20V7l6-3 6 3v13M10 10h4M10 14h4" fill="none" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" />
                    </svg>
                  </span>
                  <select
                    value={plant}
                    onChange={(e) => onPlantChange(e.target.value)}
                    className="app-shell-select"
                    disabled={optionsLoading}
                  >
                    <option value="">{selectorPlaceholder}</option>
                    {options.map((p) => (
                      <option key={p} value={p}>
                        {p}
                      </option>
                    ))}
                  </select>
                </label>
              ) : null}
              <button
                type="button"
                onClick={handleLogout}
                className="app-shell-logout app-shell-logout-header"
              >
                <span className="app-shell-logout-icon" aria-hidden="true">
                  <svg viewBox="0 0 24 24">
                    <path d="M10 6H6a2 2 0 0 0-2 2v8a2 2 0 0 0 2 2h4M14 8l4 4-4 4M18 12H9" fill="none" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round" />
                  </svg>
                </span>
                Logout
              </button>
            </div>
            </div>
          </div>
        </header>

        <main className="app-shell-content">{children}</main>
      </div>
    </div>
  )
}
