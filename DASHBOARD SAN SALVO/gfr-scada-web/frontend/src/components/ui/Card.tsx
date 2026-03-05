import type { HTMLAttributes } from 'react'

type DivProps = HTMLAttributes<HTMLDivElement>
type HeadingProps = HTMLAttributes<HTMLHeadingElement>

function joinClassNames(...values: Array<string | undefined>) {
  return values.filter(Boolean).join(' ')
}

export function Card({ className, ...props }: DivProps) {
  return (
    <div
      className={joinClassNames(
        'rounded-lg border border-slate-200 bg-white shadow-sm',
        className
      )}
      {...props}
    />
  )
}

export function CardHeader({ className, ...props }: DivProps) {
  return <div className={joinClassNames('border-b border-slate-200 px-5 py-4', className)} {...props} />
}

export function CardTitle({ className, ...props }: HeadingProps) {
  return <h3 className={joinClassNames('text-base font-semibold text-slate-900', className)} {...props} />
}

export function CardContent({ className, ...props }: DivProps) {
  return <div className={joinClassNames('px-5 py-4', className)} {...props} />
}
