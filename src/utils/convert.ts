export const decode = (raw?: Record<string, any>) =>
  raw &&
  Object.fromEntries(
    Object.entries(raw).map(([k, v]) => {
      if (typeof v === 'object' && v.wrapperName === 'Set') v = v.values
      return [k, v]
    })
  )
