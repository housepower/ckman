export function lineFeed(str: string) {
  str = str.replace(/\r\n/g, '');
  str = str.replace(/\n/g, '');
  const isDotEnd = str.endsWith(',');
  const ipList = str.split(',');
  return isDotEnd ? ipList.slice(0, ipList.length - 1) : ipList;;
}
