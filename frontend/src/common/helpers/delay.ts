export function delay(duration = 0) {
  return new Promise(resolve => setTimeout(resolve, duration));
}
