export function serializeMap(map: Map<any, any>): Map<string, any> {
  const m = new Map()
  for (let [key, value] of map) {
    m.set(JSON.stringify(key), value)
  }
  return m
}
