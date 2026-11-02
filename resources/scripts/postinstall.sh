#!/bin/sh

mkdir -p /var/log/ckman /run/ckman
chown -R ckman:ckman /var/log/ckman /etc/ckman /run/ckman
chmod 750 /var/log/ckman /etc/ckman

/bin/systemctl daemon-reload
/bin/systemctl enable ckman

if [ -f /etc/ckman/conf/ckman.yaml ]; then
	/usr/local/bin/yaml2json /etc/ckman/conf/ckman.yaml > /etc/ckman/conf/ckman.hjson
	mv /etc/ckman/conf/ckman.yaml /etc/ckman/conf/ckman.yaml.rpmsave
fi

# Initialize the persistent admin account before the service starts so the
# one-time password appears in the package install output. The command is
# idempotent: upgrades preserve the existing account and print no password.
if command -v runuser >/dev/null 2>&1; then
	if ! runuser -u ckman -- /usr/local/bin/ckman --bootstrap-admin \
		-c=/etc/ckman/conf/ckman.hjson -l=/var/log/ckman/ckman.log; then
		echo "CKMAN admin initialization deferred. Start ckman after fixing its configuration, then check journalctl -u ckman for the one-time password." >&2
	fi
else
	echo "runuser is unavailable. Start ckman to create the admin account; check journalctl -u ckman for the one-time password." >&2
fi
