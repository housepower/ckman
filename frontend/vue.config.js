'use strict';

const commonConfig = require('./src/common/configureWebpack');

module.exports = {
  ...commonConfig,
  devServer: {
    proxy: {
      '/api/login': {
        target: 'http://192.168.21.73:8808',
        logLevel: 'debug',
        // pathRewrite: { '^/api/login': '/api/login' },
        changeOrigin: true,
        secure: false,
        onProxyRes(proxyRes) {
          proxyRes.headers['Cache-Control'] = 'no-cache, no-store, must-revalidate';
        },
      },
      '/api/v1': {
        target: 'http://192.168.21.73:8808',
        logLevel: 'debug',
        // pathRewrite: { '^/api/v1': '/api/v1' },
        changeOrigin: true,
        secure: false,
        onProxyRes(proxyRes) {
          proxyRes.headers['Cache-Control'] = 'no-cache, no-store, must-revalidate';
        },
      },
    },
  },
};
