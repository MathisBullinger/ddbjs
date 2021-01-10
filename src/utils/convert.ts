export const decode = (raw?: Record<string, any>) =>
  raw &&
  Object.fromEntries(
    Object.entries(raw).map(([k, v]) => {
      if (typeof v === 'object')
        if (v.wrapperName === 'Set') v = v.values
        else if (!Array.isArray(v)) v = decode(v)
      return [k, v]
    })
  )
