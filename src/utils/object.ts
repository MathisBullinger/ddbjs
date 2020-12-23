export const mapKeys = <T extends Record<string, any>>(
  obj: T,
  func: (key: keyof T) => string
) =>
  Object.fromEntries(
    Object.entries(obj)
      .map(([k, v]) => [func(k), v])
      .filter(([k]) => k !== undefined)
  )

export const mapValues = <T extends Record<string, any>>(
  obj: T,
  func: <K extends keyof T>(value: T[K], key: K) => any
): { [K in keyof T]: any } =>
  Object.fromEntries(
    Object.entries(obj)
      .map(([k, v]) => [k, func(v, k)])
      .filter(([k]) => k !== undefined)
  )
