export const hasOwn: (obj: Record<PropertyKey, any>, key: PropertyKey) => boolean
  = Object.call.bind(Object.prototype.hasOwnProperty);
