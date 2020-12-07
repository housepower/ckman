declare let __SERVICE_NAME__: string;
declare let __PROJECT_NAME__: string;
declare let __PROJECT_DISPLAY_NAME__: string;
declare let __PROJECT_ENVIRONMENT__: 'dev' | 'prod';
declare let __PROJECT_VERSION__: string;
declare let __PROJECT_COMMITS_SINCE_RELEASE__: number;
declare let __PROJECT_COMPILE_TIME__: number;
declare let __PROJECT_COMMIT_SHA__: string;

export const Project = Object.freeze({
  serviceName: __SERVICE_NAME__,
  name: __PROJECT_NAME__,
  displayName: __PROJECT_DISPLAY_NAME__,
  environment: __PROJECT_ENVIRONMENT__,
  version: __PROJECT_VERSION__,
  commitsSinceRelease: __PROJECT_COMMITS_SINCE_RELEASE__,
  compileTime: __PROJECT_COMPILE_TIME__,
  commitSha: __PROJECT_COMMIT_SHA__,
});
