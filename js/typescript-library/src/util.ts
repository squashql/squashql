export function serializeMap(map: Map<any, any>): Map<string, any> {
  const m = new Map()
  for (const [key, value] of map) {
    m.set(JSON.stringify(key), value)
  }
  return m
}

export function serializeRecord(r: Record<string, any>): Record<string, any> {
  const m = new Map()
  for (const [key, value] of Object.entries(r)) {
    m.set(key, JSON.stringify(value))
  }
  return m
}
