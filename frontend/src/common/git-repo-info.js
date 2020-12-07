'use strict';

const cp = require('child_process');

module.exports = function getRepoInfo(cwd = process.cwd()) {
  try {
    const execConfig = { encoding: 'utf8', cwd };
    const detailString = cp.execSync(`git show --format='%H%n%h%n%cn%n%cI%n%an%n%aI%n%s' -q --encoding=UTF-8`, execConfig);
    const [sha, abbreviatedSha, committer, committerDate, author, authorDate, commitMessage] = detailString.split('\n');
    const branch = (() => {
      try {
        return cp.execSync('git symbolic-ref --short HEAD', execConfig).trim();
      } catch (e) {
        return null; // ref HEAD is not a symbolic ref
      }
    })();
    const tagString = cp.execSync('git describe --tags --long --always', execConfig).trim();
    const [, lastTag, commitsSinceLastTag] = /^(.*)-(\d+)-\w+$/.exec(tagString) || [null, null, Infinity];

    return {
      branch,
      sha,
      abbreviatedSha,
      tag: +commitsSinceLastTag === 0 ? lastTag : null,
      lastTag,
      commitsSinceLastTag: +commitsSinceLastTag,
      committer,
      committerDate,
      author,
      authorDate,
      commitMessage,
    };
  } catch (e) {
    return null;
  }
};
