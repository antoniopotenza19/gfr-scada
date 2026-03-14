interface RangePresetSelectorItem {
  key: string
  label: string
}

interface RangePresetSelectorProps {
  presets: RangePresetSelectorItem[]
  activeKey: string | null
  onSelect: (key: string) => void
}

const SHORT_RANGE_KEYS = new Set(['5m', '15m', '30m', '1h', '3h'])
const LONG_RANGE_KEYS = new Set(['1d', '1w', '1mo', '3mo', '1y', '3y'])

function presetSymbol(key: string) {
  if (key === '5m' || key === '15m' || key === '30m' || key === '1h' || key === '3h') return 'clock'
  return 'calendar'
}

function ClockIcon() {
  return (
    <svg viewBox="0 0 24 24" aria-hidden="true" className="h-4.5 w-4.5">
      <path d="M12 7v5l3 2m6-2a9 9 0 1 1-2.64-6.36" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" />
    </svg>
  )
}

function CalendarIcon() {
  return (
    <svg viewBox="0 0 24 24" aria-hidden="true" className="h-4.5 w-4.5">
      <path d="M7 3.5v2M17 3.5v2M4 8h16M6 5.5h12A2 2 0 0 1 20 7.5v10A2 2 0 0 1 18 19.5H6A2 2 0 0 1 4 17.5v-10a2 2 0 0 1 2-2Z" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" />
    </svg>
  )
}

export default function RangePresetSelector({
  presets,
  activeKey,
  onSelect,
}: RangePresetSelectorProps) {
  const shortPresets = presets.filter((preset) => SHORT_RANGE_KEYS.has(preset.key))
  const longPresets = presets.filter((preset) => LONG_RANGE_KEYS.has(preset.key))

  const renderPresetButton = (preset: RangePresetSelectorItem) => {
    const active = preset.key === activeKey
    return (
      <button
        key={preset.key}
        type="button"
        onClick={() => onSelect(preset.key)}
        className={[
          'inline-flex min-h-[40px] items-center justify-center gap-2.5 rounded-full border px-4 py-1.5 text-[13.5px] font-semibold transition',
          active
            ? 'border-sky-700 bg-sky-700 text-white shadow-[0_14px_30px_-18px_rgba(3,105,161,0.8)]'
            : 'border-slate-200 bg-white text-slate-800 shadow-[0_10px_22px_-20px_rgba(15,23,42,0.4)] hover:border-slate-300 hover:bg-slate-50 hover:text-slate-950',
        ].join(' ')}
      >
        <span
          className={[
            'inline-flex h-7 w-7 items-center justify-center rounded-full border',
            active
              ? 'border-white/20 bg-white/12 text-white'
              : 'border-slate-200 bg-slate-50 text-slate-600 shadow-inner',
          ].join(' ')}
        >
          {presetSymbol(preset.key) === 'clock' ? <ClockIcon /> : <CalendarIcon />}
        </span>
        <span className="leading-none">{preset.label}</span>
      </button>
    )
  }

  return (
    <div className="flex flex-wrap items-center gap-2.5 xl:flex-nowrap xl:gap-2.5">
      <div className="flex flex-wrap items-center gap-1.5 rounded-2xl border border-slate-200/80 bg-white/95 px-2 py-2 shadow-[0_18px_36px_-28px_rgba(15,23,42,0.35)] xl:flex-nowrap">
        {shortPresets.map(renderPresetButton)}
      </div>

      <div className="hidden h-10 w-px bg-slate-200 xl:block" aria-hidden="true" />

      <div className="flex flex-wrap items-center gap-1.5 rounded-2xl border border-slate-200/80 bg-white/95 px-2 py-2 shadow-[0_18px_36px_-28px_rgba(15,23,42,0.35)] xl:flex-nowrap">
        {longPresets.map(renderPresetButton)}
      </div>
    </div>
  )
}
