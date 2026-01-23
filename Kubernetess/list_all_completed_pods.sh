#!/bin/sh

kubectl get nodes -o custom-columns=NAME:.metadata.name | kubectl get pods -o wide --all-namespaces | grep Completed | wc
kubectl get nodes -o custom-columns=NAME:.metadata.name | kubectl get pods -o wide --all-namespaces  | grep Running | wc

kubectl get nodes -o custom-columns=NAME:.metadata.name --no-headers | while read node; do
  echo "Node: $node"
  kubectl get pods --all-namespaces --field-selector spec.nodeName=$node -o wide | grep Completed | wc
  echo ""
done
