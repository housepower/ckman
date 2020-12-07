/**
 * Convert array buffer to base64 string
 * @param {ArrayBuffer} arrayBuffer
 */
function base64ArrayBuffer(arrayBuffer) {
  let base64 = '';
  const encodings = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/';

  const bytes = new Uint8Array(arrayBuffer);
  const byteLength = bytes.byteLength;
  const byteRemainder = byteLength % 3;
  const mainLength = byteLength - byteRemainder;

  let a;
  let b;
  let c;
  let d;
  let chunk;

  // Main loop deals with bytes in chunks of 3
  for (let i = 0; i < mainLength; i += 3) {
    // Combine the three bytes into a single integer
    chunk = (bytes[i] << 16) | (bytes[i + 1] << 8) | bytes[i + 2];

    // Use bitmasks to extract 6-bit segments from the triplet
    a = (chunk & 16515072) >> 18; // 16515072 = (2^6 - 1) << 18
    b = (chunk & 258048) >> 12; // 258048   = (2^6 - 1) << 12
    c = (chunk & 4032) >> 6; // 4032     = (2^6 - 1) << 6
    d = chunk & 63;        // 63       = 2^6 - 1

    // Convert the raw binary segments to the appropriate ASCII encoding
    base64 += encodings[a] + encodings[b] + encodings[c] + encodings[d];
  }

  // Deal with the remaining bytes and padding
  if (byteRemainder === 1) {
    chunk = bytes[mainLength];

    a = (chunk & 252) >> 2; // 252 = (2^6 - 1) << 2

    // Set the 4 least significant bits to zero
    b = (chunk & 3) << 4; // 3   = 2^2 - 1

    base64 += `${encodings[a]}${encodings[b]}==`;
  } else if (byteRemainder === 2) {
    chunk = (bytes[mainLength] << 8) | bytes[mainLength + 1];

    a = (chunk & 64512) >> 10; // 64512 = (2^6 - 1) << 10
    b = (chunk & 1008) >> 4; // 1008  = (2^6 - 1) << 4

    // Set the 2 least significant bits to zero
    c = (chunk & 15) << 2; // 15    = 2^4 - 1

    base64 += `${encodings[a]}${encodings[b]}${encodings[c]}=`;
  }

  return base64;
}

/**
 * Convert remote url to data-uri
 * @param {string} styleString
 */
async function fillUrl(styleString, baseUrl = '', onlyType = '.ttf') {
  let lastIndex = 0;
  let result = '';

  const re = /url\((?:["']?)(.+?)(?:["'])?\)/g;
  for (let matches; matches = re.exec(styleString);) {
    const url = matches[1];
    if (url.startsWith('data:')) continue;
    if (onlyType && !url.endsWith(onlyType)) continue;
    const resp = await fetch(baseUrl + url);
    const dataUrl = `data:${resp.headers.get('Content-Type')};base64,${base64ArrayBuffer(await resp.arrayBuffer())}`;
    result += styleString.slice(lastIndex, matches.index);
    result += `url("${dataUrl}")`;
    lastIndex = re.lastIndex;
  }

  result += styleString.slice(lastIndex);

  return result;
}

/**
 * Export HTML element to svg
 * @param {HTMLElement} element
 * @param {object} styles
 * @param {object} size
 */
export async function html2svg(element, styles, size = {
  width: String(element.scrollWidth),
  height: String(element.scrollHeight),
}) {
  const svg = document.createElementNS('http://www.w3.org/2000/svg', 'svg');
  svg.setAttribute('width', size.width);
  svg.setAttribute('height', size.height);
  svg.setAttribute('class', document.documentElement.className);

  const title = document.createElementNS('http://www.w3.org/2000/svg', 'title');
  title.innerHTML = document.title;
  svg.appendChild(title);

  await Promise.all(Array.from(document.head.children).map(async el => {
    switch (el.tagName) {
      case 'STYLE': {
        const style = el.cloneNode(true);
        style.innerHTML = await fillUrl(style.innerHTML);
        svg.appendChild(style);
        break;
      }

      case 'LINK': {
        if (el.rel.toLowerCase() === 'stylesheet') {
          const style = document.createElement('style');
          const resp = await fetch(el.href);
          style.innerHTML = await fillUrl(await resp.text(), el.href.slice(0, el.href.lastIndexOf('/') + 1));
          svg.appendChild(style);
        }
        break;
      }
    }
  }));

  const foreignObject = document.createElementNS('http://www.w3.org/2000/svg', 'foreignObject');
  if (styles) {
    const computedStyles = getComputedStyle(element);
    Object.assign(foreignObject.style, {
      width: '100%',
      height: '100%',
      font: computedStyles.font,
      textRendering: computedStyles.textRendering,
      webkitFontSmoothing: computedStyles.webkitFontSmoothing,
      mozOsxFontSmoothing: computedStyles.mozOsxFontSmoothing,
      color: computedStyles.color,
      direction: computedStyles.direction,
    }, styles);
  }

  Array.from(element.querySelectorAll('canvas')).forEach((el, idx) => {
    if (!el.id) {
      el.id = '$$HTML2SVG_TEMP_CANVAS_ID_' + idx;
    }
  });

  const newNode = element.cloneNode(true);
  Array.from(newNode.querySelectorAll('canvas')).forEach(el => {
    const canvas = document.getElementById(el.id);
    const image = new Image();
    image.src = canvas.toDataURL('image/png');
    image.className = canvas.className;
    image.setAttribute('style', canvas.getAttribute('style'));
    el.replaceWith(image);
  });

  foreignObject.appendChild(newNode);
  svg.appendChild(foreignObject);

  return svg;
}
