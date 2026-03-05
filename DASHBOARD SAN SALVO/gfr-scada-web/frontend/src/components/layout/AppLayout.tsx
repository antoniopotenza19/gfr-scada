import { useEffect, type ReactNode } from 'react'
import { Link, NavLink, useLocation, useNavigate } from 'react-router-dom'
import { usePlants } from '../../hooks/usePlants'
import { canViewDevFeatures, getAuthUserFromSessionToken } from '../../utils/auth'

interface AppLayoutProps {
  title: string
  subtitle: string
  plant: string
  onPlantChange: (plant: string) => void
  selectorOptions?: string[]
  selectorPlaceholder?: string
  scadaPlant?: string
  children: ReactNode
}

interface SidebarItem {
  label: string
  to?: string
}

function SidebarLink({ item }: { item: SidebarItem }) {
  if (!item.to) {
    return (
      <span className="flex items-center gap-3 rounded-r-md border-l-2 border-transparent px-4 py-2 text-sm text-slate-500">
        <span className="h-2 w-2 rounded-full bg-slate-600" />
        {item.label}
      </span>
    )
  }

  return (
    <NavLink
      to={item.to}
      className={({ isActive }) =>
        [
          'flex items-center gap-3 rounded-r-md border-l-2 px-4 py-2 text-sm transition-colors',
          isActive
            ? 'border-cyan-400 bg-slate-800 text-slate-100'
            : 'border-transparent text-slate-300 hover:bg-slate-800 hover:text-slate-100',
        ].join(' ')
      }
    >
      <span className="h-2 w-2 rounded-full bg-slate-500" />
      {item.label}
    </NavLink>
  )
}

export default function AppLayout({
  title,
  subtitle,
  plant,
  onPlantChange,
  selectorOptions,
  selectorPlaceholder = 'Select plant',
  scadaPlant,
  children,
}: AppLayoutProps) {
  const navigate = useNavigate()
  const location = useLocation()
  const { data: plants, isLoading } = usePlants()
  const user = getAuthUserFromSessionToken()
  const options = selectorOptions ?? plants ?? []
  const optionsLoading = selectorOptions ? false : isLoading

  useEffect(() => {
    if (plant && options.length > 0 && !options.includes(plant)) {
      onPlantChange('')
    }
  }, [options, plant, onPlantChange])

  const handleLogout = () => {
    sessionStorage.removeItem('gfr_token')
    navigate('/login')
  }

  const isScadaRoute = location.pathname.startsWith('/scada/')
  const scadaName = scadaPlant ?? plant
  const scadaTarget = scadaName ? `/scada/${scadaName}` : undefined

  const items: SidebarItem[] = [
    { label: 'Impianti', to: '/sites' },
    { label: 'Dashboard', to: '/dashboard' },
    { label: 'SCADA', to: isScadaRoute ? location.pathname : scadaTarget },
    { label: 'Alarms', to: '/alarms' },
  ]
  if (canViewDevFeatures(user)) items.push({ label: 'Dev', to: '/dev' })

  return (
    <div className="min-h-screen bg-slate-50 text-slate-900">
      <aside className="fixed inset-y-0 left-0 w-64 bg-gradient-to-b from-slate-900 to-slate-950 text-slate-100">
        <div className="border-b border-slate-800 px-6 py-5">
          <Link to="/dashboard" className="block">
            <div className="text-lg font-semibold tracking-wide">GFR SCADA</div>
            <div className="text-xs text-slate-400">Enterprise Monitoring</div>
          </Link>
        </div>

        <nav className="mt-4 space-y-1 px-2">
          {items.map((item) => (
            <SidebarLink key={item.label} item={item} />
          ))}
        </nav>

        <div className="absolute inset-x-0 bottom-0 border-t border-slate-800 px-4 py-4">
          <div className="mb-2 rounded-md bg-slate-800/80 px-3 py-2">
            <div className="text-xs text-slate-400">Signed in as</div>
            <div className="text-sm font-medium text-slate-100">{user.role}</div>
          </div>
          <button
            type="button"
            onClick={handleLogout}
            className="w-full rounded-md border border-slate-700 px-3 py-2 text-sm text-slate-200 hover:bg-slate-800"
          >
            Logout
          </button>
        </div>
      </aside>

      <div className="pl-64">
        <header className="border-b border-slate-200 bg-white">
          <div className="flex items-center justify-between px-6 py-4">
            <div>
              <h1 className="text-xl font-semibold text-slate-900">{title}</h1>
              <p className="text-sm text-slate-500">{subtitle}</p>
            </div>

            <div className="flex items-center gap-3">
              <select
                value={plant}
                onChange={(e) => onPlantChange(e.target.value)}
                className="h-9 rounded-md border border-slate-300 bg-white px-3 text-sm text-slate-700 outline-none focus:border-slate-400"
                disabled={optionsLoading}
              >
                <option value="">{selectorPlaceholder}</option>
                {options.map((p) => (
                  <option key={p} value={p}>
                    {p}
                  </option>
                ))}
              </select>
              <button
                type="button"
                onClick={handleLogout}
                className="h-9 rounded-md border border-slate-300 px-4 text-sm font-medium text-slate-700 hover:bg-slate-100"
              >
                Logout
              </button>
            </div>
          </div>
        </header>

        <main className="p-6">{children}</main>
      </div>
    </div>
  )
}
