import AppLayout from '../components/layout/AppLayout'

export default function DevTools() {
  return (
    <AppLayout
      title="Dev Tools"
      subtitle="Strumenti disponibili solo per ruolo dev"
      plant=""
      onPlantChange={() => {}}
      selectorOptions={[]}
      selectorPlaceholder="N/A"
    >
      <div className="rounded-lg border border-slate-200 bg-white p-5 text-sm text-slate-700">
        Area riservata agli strumenti di sviluppo.
      </div>
    </AppLayout>
  )
}
