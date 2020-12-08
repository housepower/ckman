'use strict';

const path = require('path');
const fs = require('fs');
const { DefinePlugin } = require('webpack');
const repoInfo = require('./git-repo-info')();

let rootPath = __dirname;

do {
  rootPath = path.resolve(rootPath, '..');
  if (rootPath === '/') throw new Error('package.json is NOT found');
} while (!fs.existsSync(path.resolve(rootPath, 'package.json')));

const packageJson = require(path.join(rootPath, 'package.json'));

module.exports = {
  ...(() => (packageJson.serviceName ? { publicPath: `/${packageJson.serviceName}/` } : {}))(),
  transpileDependencies: process.env.NODE_ENV === 'production' ? [/node_modules/] : [],
  css: {
    loaderOptions: {
      sass: {
        additionalData: `$env: ${process.env.NODE_ENV};\n@import "~@/app/variables";`,
      },
      less: {
        lessOptions: {
          javascriptEnabled: true,
        },
      },
    },
  },
  configureWebpack: {
    plugins: [
      new DefinePlugin({
        __SERVICE_NAME__: JSON.stringify(packageJson.serviceName || null),
        __PROJECT_NAME__: JSON.stringify(packageJson.name),
        __PROJECT_DISPLAY_NAME__: JSON.stringify(packageJson.displayName),
        __PROJECT_COMMIT_SHA__: JSON.stringify((repoInfo && repoInfo.sha) || null),
        __PROJECT_VERSION__: JSON.stringify((repoInfo && repoInfo.lastTag) || null),
        __PROJECT_COMMITS_SINCE_RELEASE__: JSON.stringify(
          (repoInfo && repoInfo.commitsSinceLastTag) || null
        ),
        __PROJECT_ENVIRONMENT__: JSON.stringify(
          process.env.NODE_ENV === 'production' ? 'prod' : 'dev'
        ),
        __PROJECT_COMPILE_TIME__: JSON.stringify(Date.now()),
      }),
    ],
    resolve: {
      alias: {
        '@images': path.resolve(rootPath, 'src/assets/images'),
        '@fonts': path.resolve(rootPath, 'src/assets/fonts'),
        '@videos': path.resolve(rootPath, 'src/assets/videos'),
        'element-ui': 'element-ui-eoi',
      },
      mainFields: ['module', 'main'],
    },
    devtool: process.env.NODE_ENV === 'development' && 'source-map',
  },
  /** @param {{
   *   optimization: import('webpack-chain').Optimization,
   *   resolve: import('webpack-chain').Resolve
   * }} config */
  chainWebpack(config) {
    const maxSize = (process.env.NODE_ENV === 'development' ? 2 : 8) * 1024 * 1024;
    config.optimization.merge({
      splitChunks: {
        cacheGroups: {
          vendors: { maxSize },
          common: { maxSize },
        },
      },
    });
    config.resolve.extensions
      .clear()
      .merge(['.vue', '.tsx', '.ts', '.mjs', '.js', '.jsx', '.json', '.wasm']);
  },
};
