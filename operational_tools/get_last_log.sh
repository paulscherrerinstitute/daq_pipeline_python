#!/usr/bin/env bash

kubectl get pods --insecure-skip-tls-verify="true" | awk '{print $1}' | xargs -t -L 1 kubectl logs --tail=1 --insecure-skip-tls-verify="true"

