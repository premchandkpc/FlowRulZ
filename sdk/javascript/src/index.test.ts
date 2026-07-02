import { describe, it, expect } from "vitest"
import {
  FlowRulZClient,
  MODE_PUBLISH,
  MODE_REQUEST,
  MODE_REPLY,
  MODE_STREAM,
  MODE_WORKFLOW,
  MODE_INTERNAL,
} from "./index"

describe("FlowRulZClient", () => {
  it("creates with default config", () => {
    const c = new FlowRulZClient()
    expect(c).toBeInstanceOf(FlowRulZClient)
  })

  it("creates with custom address", () => {
    const c = new FlowRulZClient({ address: "http://localhost:9090" })
    expect(c).toBeInstanceOf(FlowRulZClient)
  })
})

describe("mode constants", () => {
  it("have correct values", () => {
    expect(MODE_PUBLISH).toBe(0)
    expect(MODE_REQUEST).toBe(1)
    expect(MODE_REPLY).toBe(2)
    expect(MODE_STREAM).toBe(3)
    expect(MODE_WORKFLOW).toBe(4)
    expect(MODE_INTERNAL).toBe(5)
  })
})
