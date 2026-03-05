import type { SynopticMachineStatus } from './MachineNode'

interface PipeNetworkProps {
  machineStates: Record<string, SynopticMachineStatus>
  layout?: 'default' | 'laminato'
}

function isProducing(status: SynopticMachineStatus) {
  return status === 'active'
}

function collectorClass(active: boolean) {
  return active ? 'collector-active' : 'collector-idle'
}

function branchClass(active: boolean) {
  return active ? 'branch-active' : 'branch-idle'
}

function branchClassReverse(active: boolean) {
  return active ? 'branch-active-reverse' : 'branch-idle'
}

function FlowStyles() {
  return (
    <style>
      {`
        .collector-idle {
          stroke: #cbd5e1;
          stroke-width: 8;
          fill: none;
          stroke-linecap: round;
          stroke-linejoin: round;
        }
        .collector-active {
          stroke: #22c55e;
          stroke-width: 8;
          fill: none;
          stroke-linecap: round;
          stroke-linejoin: round;
          stroke-dasharray: 12 8;
          animation: dash-flow 1.2s linear infinite;
        }
        .branch-idle {
          stroke: #cbd5e1;
          stroke-width: 8;
          fill: none;
          stroke-linecap: round;
          stroke-linejoin: round;
        }
        .branch-active {
          stroke: #22c55e;
          stroke-width: 8;
          fill: none;
          stroke-linecap: round;
          stroke-linejoin: round;
          stroke-dasharray: 10 6;
          animation: dash-flow 1s linear infinite;
        }
        .branch-active-reverse {
          stroke: #22c55e;
          stroke-width: 8;
          fill: none;
          stroke-linecap: round;
          stroke-linejoin: round;
          stroke-dasharray: 10 6;
          animation: dash-flow-reverse 1s linear infinite;
        }
        @keyframes dash-flow {
          from { stroke-dashoffset: 0; }
          to { stroke-dashoffset: -16; }
        }
        @keyframes dash-flow-reverse {
          from { stroke-dashoffset: 0; }
          to { stroke-dashoffset: 16; }
        }
      `}
    </style>
  )
}

export default function PipeNetwork({ machineStates, layout = 'default' }: PipeNetworkProps) {
  if (layout === 'laminato') {
    const cols = [170, 610, 1030]
    const branchesOn = [
      isProducing(machineStates.C1 || 'offline'),
      isProducing(machineStates.C2 || 'offline'),
      isProducing(machineStates.C3 || 'offline'),
    ]
    const anyOn = branchesOn.some(Boolean)
    const collectorY = 220
    const collectorStartX = cols[0]
    const collectorEndX = cols[cols.length - 1]
    const dryerY = 122
    const dryerX = 610
    const dryerOutletY = 87
    const dryerOutletStartX = 580
    const dryerOutletEndX = 1320
    const boilerTopY = 266
    const boilerBottomY = 300
    const machineTopY = 382
    const activeMachineXs = cols.filter((_, idx) => branchesOn[idx])
    const activeXs = anyOn ? [...activeMachineXs, dryerX].sort((a, b) => a - b) : []
    const activeCollectorStartX = activeXs[0] ?? collectorStartX
    const activeCollectorEndX = activeXs[activeXs.length - 1] ?? collectorEndX

    return (
      <g>
        <FlowStyles />

        <path d={`M${collectorStartX} ${collectorY} L${collectorEndX} ${collectorY}`} className={collectorClass(false)} />
        {anyOn ? <path d={`M${activeCollectorStartX} ${collectorY} L${activeCollectorEndX} ${collectorY}`} className={collectorClass(true)} /> : null}
        <path d={`M${dryerX} ${collectorY} L${dryerX} ${dryerY}`} className={branchClass(anyOn)} />
        <path d={`M${dryerOutletStartX} ${dryerOutletY} L${dryerOutletEndX} ${dryerOutletY}`} className={branchClass(anyOn)} />

        {cols.map((x, idx) => (
          <g key={`up-${x}`}>
            <path d={`M${x} ${collectorY} L${x} ${boilerTopY}`} className={idx === 0 ? branchClassReverse(branchesOn[idx]) : branchClass(branchesOn[idx])} />
            <path d={`M${x} ${boilerBottomY} L${x} ${machineTopY}`} className={idx === 0 ? branchClassReverse(branchesOn[idx]) : branchClass(branchesOn[idx])} />
          </g>
        ))}
      </g>
    )
  }

  const cols = [200, 500, 800]
  const branchesOn = [
    isProducing(machineStates.M1 || 'offline'),
    isProducing(machineStates.M2 || 'offline'),
    isProducing(machineStates.V1 || 'offline'),
  ]
  const anyOn = branchesOn.some(Boolean)
  const collectorY = 230
  const collectorStartX = cols[0]
  const collectorEndX = cols[cols.length - 1]
  const dryerY = 128
  const dryerX = 500
  const dryerOutletY = 87
  const dryerOutletStartX = 580
  const dryerOutletEndX = 1320
  const machineTopY = 340
  const machineBottomY = 398
  const tankTopY = 512
  const activeMachineXs = cols.filter((_, idx) => branchesOn[idx])
  const activeXs = anyOn ? [...activeMachineXs, dryerX].sort((a, b) => a - b) : []
  const activeCollectorStartX = activeXs[0] ?? collectorStartX
  const activeCollectorEndX = activeXs[activeXs.length - 1] ?? collectorEndX

  return (
    <g>
      <FlowStyles />

      <path d={`M${collectorStartX} ${collectorY} L${collectorEndX} ${collectorY}`} className={collectorClass(false)} />
      {anyOn ? <path d={`M${activeCollectorStartX} ${collectorY} L${activeCollectorEndX} ${collectorY}`} className={collectorClass(true)} /> : null}
      <path d={`M${dryerX} ${collectorY} L${dryerX} ${dryerY}`} className={branchClass(anyOn)} />
      <path d={`M${dryerOutletStartX} ${dryerOutletY} L${dryerOutletEndX} ${dryerOutletY}`} className={branchClass(anyOn)} />

      {cols.map((x, idx) => (
        <g key={`default-${x}`}>
          <path d={`M${x} ${collectorY} L${x} ${machineTopY}`} className={branchClass(branchesOn[idx])} />
          <path d={`M${x} ${machineBottomY} L${x} ${tankTopY}`} className={branchClass(branchesOn[idx])} />
        </g>
      ))}
    </g>
  )
}
