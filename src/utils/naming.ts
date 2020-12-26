import reserved from '../reserved'

const reservedLength = Array.from(new Set(reserved.map(v => v.length)))

export const valid = (name: string): boolean => {
  if (name.length === 0) return false
  if (!reservedLength.includes(name.length)) return true
  return !reserved.includes(name.toUpperCase())
}
