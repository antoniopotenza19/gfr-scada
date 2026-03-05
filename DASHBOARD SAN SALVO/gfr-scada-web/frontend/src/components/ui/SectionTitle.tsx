import type { ReactNode } from 'react'

interface SectionTitleProps {
  title: string
  subtitle?: string
  action?: ReactNode
}

export default function SectionTitle({ title, subtitle, action }: SectionTitleProps) {
  return (
    <div className="flex flex-col gap-3 sm:flex-row sm:items-end sm:justify-between">
      <div>
        <h2 className="text-lg font-semibold text-slate-900">{title}</h2>
        {subtitle ? <p className="text-sm text-slate-500">{subtitle}</p> : null}
      </div>
      {action ? <div>{action}</div> : null}
    </div>
  )
}
