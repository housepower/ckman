run ckman on k8s:

```
set -a
. ./ckman-env.conf
envsubst < k8s-ckman.yaml |kubectl -f apply -f -
```
