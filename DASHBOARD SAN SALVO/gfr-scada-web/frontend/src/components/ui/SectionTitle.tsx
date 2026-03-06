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
        <h2 className="text-[18px] font-semibold leading-[1.4] tracking-[-0.01em] text-slate-900">{title}</h2>
        {subtitle ? <p className="text-[13px] font-normal leading-[1.5] tracking-[0.2px] text-[#6b7a8c]">{subtitle}</p> : null}
      </div>
      {action ? <div>{action}</div> : null}
    </div>
  )
}
