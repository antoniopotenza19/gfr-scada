import { Link } from 'react-router-dom'

export default function NotAuthorized() {
  return (
    <div className="flex min-h-screen items-center justify-center bg-slate-50 px-4">
      <div className="w-full max-w-md rounded-lg border border-slate-200 bg-white p-6 text-center shadow-sm">
        <h1 className="text-2xl font-semibold text-slate-900">403</h1>
        <p className="mt-2 text-sm text-slate-600">Non sei autorizzato ad accedere a questa pagina.</p>
        <div className="mt-4">
          <Link to="/sites" className="rounded-md border border-slate-300 bg-white px-3 py-2 text-sm text-slate-700 hover:bg-slate-50">
            Torna agli impianti
          </Link>
        </div>
      </div>
    </div>
  )
}
