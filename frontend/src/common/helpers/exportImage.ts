import { html2svg } from './html2svg';

export function exportImage(el: HTMLElement, fileName: string, styles: Record<string, any> = {
  backgroundColor: getComputedStyle(document.body).backgroundColor,
}) {
  return html2svg(el, styles, {
    width: el.scrollWidth + 32,
    height: el.scrollHeight + 16,
  })
    .then(svg => {
      const svgContent = new XMLSerializer().serializeToString(svg);
      return new Promise<HTMLImageElement>((resolve, reject) => {
        const img = new Image();
        img.onload = () => resolve(img);
        img.onerror = e => reject(e);
        img.src = 'data:image/svg+xml,' + encodeURIComponent(svgContent);
      });
    })
    .then(img => {
      const canvas = document.createElement('canvas');
      const devicePixelRatio = window.devicePixelRatio || 1;
      canvas.width = img.naturalWidth * devicePixelRatio;
      canvas.height = img.naturalHeight * devicePixelRatio;
      const ctx = canvas.getContext('2d');
      ctx.scale(devicePixelRatio, devicePixelRatio);
      ctx.drawImage(img, 0, 0);
      return new Promise(resolve => canvas.toBlob(resolve));
    })
    .then(blob => {
      const url = URL.createObjectURL(blob);
      const a = document.createElement('a');
      a.download = fileName + '.png';
      a.href = url;
      a.click();
      URL.revokeObjectURL(url);
    });
}
