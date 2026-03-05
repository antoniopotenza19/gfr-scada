import api from './client'

export type MachineCommand = 'START' | 'STOP'

interface DirectCommandResponse {
  mode: 'direct'
  data: unknown
}

interface QueuedCommandResponse {
  mode: 'queued'
  data: unknown
}

export type SendMachineCommandResponse = DirectCommandResponse | QueuedCommandResponse

function isNotImplementedStatus(status?: number) {
  return status === 404 || status === 405 || status === 501
}

export async function sendMachineCommand(
  siteId: 'san-salvo' | 'marghera',
  roomId: string,
  machineId: string,
  command: MachineCommand
): Promise<SendMachineCommandResponse> {
  try {
    const direct = await api.post(
      `/api/sites/${encodeURIComponent(siteId)}/rooms/${encodeURIComponent(roomId)}/machines/${encodeURIComponent(machineId)}/command`,
      { command }
    )
    return { mode: 'direct', data: direct.data }
  } catch (error: any) {
    const status = Number(error?.response?.status || 0)
    if (!isNotImplementedStatus(status)) throw error
  }

  const fallback = await api.post('/api/commands/request', {
    command,
    target: `${siteId}:${roomId}:${machineId}`,
    params: JSON.stringify({ siteId, roomId, machineId }),
  })
  return { mode: 'queued', data: fallback.data }
}
