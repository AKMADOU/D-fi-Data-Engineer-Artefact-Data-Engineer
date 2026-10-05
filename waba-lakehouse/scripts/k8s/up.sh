#!/usr/bin/env bash
# Level 4 : démarre minikube (profil dédié « waba »), construit les images, génère les
# secrets et applique toute la plateforme en UNE commande kubectl.
#
# Mémoire : par défaut, tout ce que Docker met à disposition moins 1 Go. En dessous de
# 20 Go, le profil « léger » (k8s/profiles/light/patches.yaml : tas JVM, limites) est
# appliqué automatiquement. Forcer : WABA_PROFILE=full|light, MINIKUBE_MEMORY=12g.
set -euo pipefail
cd "$(dirname "$0")/../.."
PROFILE="${MINIKUBE_PROFILE:-waba}"
MK=(minikube -p "$PROFILE")

# --- ressources
DOCKER_MB=$(( $(docker info --format '{{.MemTotal}}' 2>/dev/null || echo 17179869184) / 1048576 ))
if [ -n "${MINIKUBE_MEMORY:-}" ]; then MEMORY="$MINIKUBE_MEMORY"
else
  MEM_MB=$(( DOCKER_MB - 1024 )); [ "$MEM_MB" -gt 24576 ] && MEM_MB=24576
  MEMORY="${MEM_MB}m"
fi
MEM_NUM=$(echo "$MEMORY" | tr -dc '0-9'); case "$MEMORY" in *g|*G) MEM_NUM=$((MEM_NUM*1024));; esac
if [ -z "${WABA_PROFILE:-}" ]; then
  if [ "$MEM_NUM" -lt 20000 ]; then WABA_PROFILE=light; else WABA_PROFILE=full; fi
fi
CPUS="${MINIKUBE_CPUS:-$(docker info --format '{{.NCPU}}' 2>/dev/null || echo 4)}"
[ "$CPUS" -gt 8 ] && CPUS=8
echo "Docker : ${DOCKER_MB} Mo | minikube « $PROFILE » : ${MEMORY}, ${CPUS} CPU | profil plateforme : ${WABA_PROFILE}"
if [ "$MEM_NUM" -lt 11000 ]; then
  echo "⚠  Moins de 11 Go pour minikube : la plateforme complète ne tiendra pas." >&2
  echo "   Docker Desktop > Settings > Resources > Memory : mettre le maximum possible (≥ 12 Go)." >&2
fi

# --- cluster (profil dédié : ne touche pas à un éventuel profil « minikube » existant)
if ! "${MK[@]}" status >/dev/null 2>&1; then
  # containerd : logs au format CRI ; version Kubernetes « stable » de ce minikube
  "${MK[@]}" start --cpus "$CPUS" --memory "$MEMORY" --disk-size "${MINIKUBE_DISK:-60g}" \
    --kubernetes-version="${K8S_VERSION:-stable}" --container-runtime=containerd
fi
kubectl config use-context "$PROFILE" >/dev/null
"${MK[@]}" addons enable ingress >/dev/null
"${MK[@]}" addons enable metrics-server >/dev/null || true
kubectl -n ingress-nginx rollout status deploy/ingress-nginx-controller --timeout=300s

[ "${SKIP_BUILD:-0}" = "1" ] || MINIKUBE_PROFILE="$PROFILE" ./scripts/k8s/build-images.sh
python3 scripts/k8s/render_identity.py --check
python3 scripts/k8s/make_secrets.py --profile "$WABA_PROFILE"

kubectl apply --server-side --force-conflicts -k .
echo
echo "Déploiement lancé (profil $WABA_PROFILE). Suivi : make k8s-status  (1er démarrage : 15 à 30 min)"
MINIKUBE_PROFILE="$PROFILE" ./scripts/k8s/hosts.sh
