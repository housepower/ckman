export function parseJsonSafe(json, value = json) {
  try {
    return JSON.parse(json);
  } catch {
    return value;
  }
}
