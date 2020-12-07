import { format } from 'date-fns';
import platform from 'platform';

import { Project } from '../constants';

void function printBanner() {
  console.info(String.raw`
 ______ ____ _____ _______ ______ _  __
|  ____/ __ \_   _|__   __|  ____| |/ /
| |__ | |  | || |    | |  | |__  | ' /
|  __|| |  | || |    | |  |  __| |  <
| |___| |__| || |_   | |  | |____| . \
|______\____/_____|  |_|  |______|_|\_\
`.slice(1, -1));
}();

void function detectEnv() {
  document.documentElement.classList.add('env-' + Project.environment);
  console.group('版本号');
  console.info(`${Project.displayName} (${Project.name.toUpperCase()})，运行在 ${Project.environment} 环境，编译时间：${format(Project.compileTime, `yyyy-MM-dd'T'HH:mm:ssXXXXX`)}`);
  console.info(`发布版本：${Project.version}；最后提交：${Project.commitSha}${Project.commitsSinceRelease ? `，超前发布版本 ${Project.commitsSinceRelease} 个提交` : ''}`);
  console.info('运行在：' + platform.description);
  console.groupEnd();
}();

void function detectOS() {
  let OSName = 'Unknown';
  if (navigator.appVersion.includes('Win')) {
    OSName = 'Windows';
  } else if (navigator.appVersion.includes('Mac')) {
    OSName = 'macOS';
  } else if (navigator.appVersion.includes('X11')) {
    OSName = 'UNIX';
  } else if (navigator.appVersion.includes('Linux')) {
    OSName = 'Linux';
  }

  document.documentElement.classList.add('os-' + OSName.toLowerCase());
}();

void function detectLayout() {
  document.documentElement.classList.add('platform-' + platform.layout.toLowerCase());
}();

void function detectWebpSupport() {
  const canvas = document.createElement('canvas');
  canvas.width = canvas.height = 1;
  if (canvas.toDataURL('image/webp').indexOf('image/webp') === 5) {
    document.documentElement.classList.add('webp');
  } else {
    document.documentElement.classList.add('no-webp');
  }
}();
