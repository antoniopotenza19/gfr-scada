export type CompressorStatus = 'on' | 'off' | 'alarm' | 'standby'

interface StatusBadgeProps {
  status: CompressorStatus
}

const STATUS_STYLES: Record<
  CompressorStatus,
  { label: string; text: string; border: string; bg: string; dot: string }
> = {
  on: {
    label: 'active',
    text: 'text-[#118a52]',
    border: 'border-[#9ddfb9]',
    bg: 'bg-[#e9fbf3]',
    dot: 'bg-[#58d68d]',
  },
  off: {
    label: 'off',
    text: 'text-slate-600',
    border: 'border-slate-300',
    bg: 'bg-slate-50',
    dot: 'bg-slate-400',
  },
  alarm: {
    label: 'Allarme',
    text: 'text-rose-700',
    border: 'border-rose-200',
    bg: 'bg-rose-50',
    dot: 'bg-rose-500',
  },
  standby: {
    label: 'Standby',
    text: 'text-[#996300]',
    border: 'border-[#ebcf80]',
    bg: 'bg-[#fff8df]',
    dot: 'bg-[#e2b73b]',
  },
}

export default function StatusBadge({ status }: StatusBadgeProps) {
  const style = STATUS_STYLES[status]
  return (
    <span
      className={[
        'inline-flex items-center gap-1.5 rounded-full border px-2.5 py-0.5 text-xs font-medium leading-5',
        style.border,
        style.bg,
        style.text,
      ].join(' ')}
    >
      <span className={['inline-block h-2 w-2 rounded-full', style.dot].join(' ')} />
      {style.label}
    </span>
  )
}
