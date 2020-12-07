// 注意FullScreen的大小写
if (!Element.prototype.requestFullscreen) {
  const prototype = Element.prototype;
  prototype.requestFullscreen = prototype.webkitRequestFullscreen
    || prototype.mozRequestFullScreen
    || prototype.msRequestFullscreen;
}

if (!HTMLDocument.prototype.exitFullscreen) {
  const prototype = HTMLDocument.prototype;
  prototype.exitFullscreen = prototype.webkitExitFullscreen
    || prototype.mozCancelFullScreen
    || prototype.msExitFullscreen;
}

if (!('fullscreenElement' in HTMLDocument.prototype)) {
  Object.defineProperty(HTMLDocument.prototype, 'fullscreenElement', {
    get() {
      return document.webkitFullscreenElement || document.mozFullScreenElement || document.msFullscreenElement;
    },
    enumerable: false,
    configurable: true,
  })
}
