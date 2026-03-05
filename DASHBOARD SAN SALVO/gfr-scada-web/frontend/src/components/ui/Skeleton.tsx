interface SkeletonProps {
  className?: string
}

export default function Skeleton({ className = '' }: SkeletonProps) {
  return <div className={`rounded-md bg-slate-200 ${className}`.trim()} />
}
