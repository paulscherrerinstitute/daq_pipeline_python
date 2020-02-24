N_PODS=${1:-999}
echo "Creating first" "${N_PODS}" " pipelines."
ls -ld sources/* | awk '{print $9}' | head -n ${N_PODS} | xargs -L 1 kubectl --insecure-skip-tls-verify="true" apply -f

