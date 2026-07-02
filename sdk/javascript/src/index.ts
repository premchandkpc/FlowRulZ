// -- types --

export const MODE_PUBLISH = 0
export const MODE_REQUEST = 1
export const MODE_REPLY = 2
export const MODE_STREAM = 3
export const MODE_WORKFLOW = 4
export const MODE_INTERNAL = 5

export interface Event {
  id?: string
  topic: string
  payload: unknown
  headers?: Record<string, string>
  mode: number
}

export interface Config {
  address?: string
  apiKey?: string
}

export interface ExecuteOpts {
  timeout?: number
  headers?: Record<string, string>
}

// -- client --

export class FlowRulZClient {
  private addr: string
  private apiKey: string | undefined

  constructor(cfg: Config = {}) {
    this.addr = cfg.address ?? "http://localhost:8080"
    this.apiKey = cfg.apiKey
  }

  async publish(topic: string, payload: unknown): Promise<void> {
    const evt: Event = { topic, payload: payload, mode: MODE_PUBLISH }
    await this.sendEvent(evt)
  }

  async request(
    service: string,
    payload: unknown,
    timeout?: number
  ): Promise<Uint8Array> {
    const evt: Event = { topic: service, payload, mode: MODE_REQUEST }
    const ctrl = timeout ? AbortSignal.timeout(timeout * 1000) : undefined
    return this.roundTrip(evt, ctrl)
  }

  async execute(
    ruleId: string,
    payload: unknown,
    opts?: ExecuteOpts
  ): Promise<Uint8Array> {
    const evt: Event = {
      topic: ruleId,
      payload,
      mode: MODE_WORKFLOW,
      headers: opts?.headers,
    }
    const ctrl = opts?.timeout
      ? AbortSignal.timeout(opts.timeout * 1000)
      : AbortSignal.timeout(30_000)
    return this.roundTrip(evt, ctrl)
  }

  async stream(
    topic: string,
    handler: (chunk: Uint8Array) => void
  ): Promise<void> {
    const url = `${this.addr}/stream/${topic}`
    const headers: Record<string, string> = {}
    if (this.apiKey) headers["Authorization"] = `Bearer ${this.apiKey}`
    const resp = await fetch(url, { headers })
    const reader = resp.body!.getReader()
    while (true) {
      const { done, value } = await reader.read()
      if (done) break
      handler(value)
    }
  }

  // -- internal --

  private async sendEvent(evt: Event): Promise<void> {
    const body = JSON.stringify(evt)
    const headers: Record<string, string> = {
      "Content-Type": "application/json",
      "X-FlowRulZ-Mode": String(evt.mode),
    }
    if (this.apiKey) headers["Authorization"] = `Bearer ${this.apiKey}`
    await fetch(`${this.addr}/event`, { method: "POST", body, headers })
  }

  private async roundTrip(
    evt: Event,
    signal?: AbortSignal
  ): Promise<Uint8Array> {
    const body = JSON.stringify(evt)
    const headers: Record<string, string> = {
      "Content-Type": "application/json",
      "X-FlowRulZ-Mode": String(evt.mode),
    }
    if (this.apiKey) headers["Authorization"] = `Bearer ${this.apiKey}`
    const resp = await fetch(`${this.addr}/event`, {
      method: "POST",
      body,
      headers,
      signal,
    })
    return new Uint8Array(await resp.arrayBuffer())
  }
}
