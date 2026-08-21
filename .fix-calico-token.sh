#!/bin/bash
set -e
kubectl apply -f - <<'EOF'
apiVersion: v1
kind: Secret
metadata:
  name: calico-cni-plugin-token
  namespace: kube-system
  annotations:
    kubernetes.io/service-account.name: calico-cni-plugin
type: kubernetes.io/service-account-token
EOF
sleep 3
TOKEN=$(kubectl -n kube-system get secret calico-cni-plugin-token -o jsonpath="{.data.token}" | base64 -d)
echo "TOKEN_LEN=${#TOKEN}"
if [ -z "$TOKEN" ]; then
  echo "ERROR: token empty"
  exit 1
fi
# 备份原 kubeconfig，再替换过期 token
cp -a /etc/cni/net.d/calico-kubeconfig /etc/cni/net.d/calico-kubeconfig.bak
sed -i "s|^    token: .*|    token: ${TOKEN}|" /etc/cni/net.d/calico-kubeconfig
echo "KUBECONFIG_UPDATED"
