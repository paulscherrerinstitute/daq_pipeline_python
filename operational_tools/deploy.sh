#!/usr/bin/env bash

kubectl --insecure-skip-tls-verify="true" delete -n default pod device1
kubectl --insecure-skip-tls-verify="true" apply -f pod_template.json
