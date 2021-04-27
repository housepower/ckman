# ckman
This is a tool which used to manage and monitor ClickHouse database

## build command
```bash
make package VERSION=x.x.x
```

## pprof
> http://127.0.0.1:8808/debug/pprof/

## docker run
```bash
docker run -itd -p 8808:8808 --restart unless-stopped --name ckman quay.io/housepower/ckman:latest
```
Then you can view the manage page on [localhost:8808/](localhost:8808/)    
You can run ckman in docker, but since deployment of Nacos is not ckman's responsibility,
 we do not enable Nacos by default.
 If you want to enable Nacos, please modify the configuration file in container.

## swagger
> http://127.0.0.1:8808/swagger/index.html

Please login first, it will return token when verify successfully,
then we should enter the token into "Authorize" frame,
after that we will access other APIs successfully.
Since v1.2.7,  we no longer provide the function of swagger document.
If you really want to, you can use `swagger_enable` option in the configuration file:
```yaml
swagger_enable: true
```

## Chinese document
[Ckman_Document_zh.md](docs/Ckman_Document_zh.md)

## ckmanpasswd
This is a tool to generate password for reserved user ckman,
password file will saved under conf directory
