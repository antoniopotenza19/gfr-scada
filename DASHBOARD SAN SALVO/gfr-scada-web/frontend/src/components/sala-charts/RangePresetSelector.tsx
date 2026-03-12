import type { ChartRangeKey, RangePreset } from '../../constants/salaCharts'

interface RangePresetSelectorProps {
  presets: RangePreset[]
  activeKey: ChartRangeKey | null
  onSelect: (key: ChartRangeKey) => void
}

export default function RangePresetSelector({
  presets,
  activeKey,
  onSelect,
}: RangePresetSelectorProps) {
  return (
    <div className="flex flex-wrap gap-2">
      {presets.map((preset) => {
        const active = preset.key === activeKey
        return (
          <button
            key={preset.key}
            type="button"
            onClick={() => onSelect(preset.key)}
            className={[
              'rounded-full border px-3 py-1.5 text-sm font-medium transition',
              active
                ? 'border-slate-900 bg-slate-900 text-white shadow-sm'
                : 'border-slate-200 bg-white text-slate-700 hover:border-slate-300 hover:bg-slate-50',
            ].join(' ')}
          >
            {preset.label}
          </button>
        )
      })}
    </div>
  )
}
