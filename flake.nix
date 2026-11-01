###############################################################
# flake.nix — Laboratorio de Kafka
# Python 3.12 + uv + Jupyter + Confluent-Kafka + Docker/Podman
###############################################################
{
  description = "Laboratorio Kafka — Python (uv) + Docker/Podman";

  inputs.nixpkgs.url = "github:NixOS/nixpkgs/nixos-26.05";

  outputs =
    { self, nixpkgs }:
    let
      system = "x86_64-linux";
      pkgs = import nixpkgs { inherit system; };

      # El intérprete viene de Nix; los paquetes Python vienen de requirements.txt
      # y los instala uv al entrar al shell (ver shellHook).
      pythonEnv = pkgs.python312;
    in
    {
      devShells.${system}.default = pkgs.mkShell {
        packages = [
          pythonEnv
          pkgs.uv
          # pkgs.docker-compose se omite a proposito: en este equipo `docker` es
          # el shim de podman, y si docker-compose (Go) esta en el PATH toma el
          # control y no ve los contenedores de podman ("service not running").
          # Sin el, `docker compose ...` delega en podman-compose y funciona.
          pkgs.podman-compose
          pkgs.kcat
          pkgs.jq
        ];

        LD_LIBRARY_PATH = pkgs.lib.makeLibraryPath [
          pkgs.stdenv.cc.cc.lib
          pkgs.zlib
        ];

        shellHook = ''
          PY_CURRENT="$(readlink -f "${pythonEnv}/bin/python")"
          REQ_FILE="$PWD/requirements.txt"
          REQ_HASH=""
          [ -f "$REQ_FILE" ] && REQ_HASH="$(sha256sum "$REQ_FILE" | cut -d' ' -f1)"

          # 1) .venv contra el intérprete actual de Nix
          if [ ! -d .venv ] || [ "$(readlink -f .venv/bin/python 2>/dev/null)" != "$PY_CURRENT" ]; then
            echo "→ (re)creando .venv contra $PY_CURRENT"
            rm -rf .venv
            uv venv
          fi
          source .venv/bin/activate
          export PYTHONUNBUFFERED=1

          # 2) dependencias de requirements.txt (solo si cambió el archivo)
          if [ -n "$REQ_HASH" ] && [ "$(cat .venv/.req-hash 2>/dev/null)" != "$REQ_HASH" ]; then
            echo "→ instalando requirements.txt con uv ..."
            if uv pip install -r "$REQ_FILE"; then
              printf '%s\n' "$REQ_HASH" > .venv/.req-hash
            else
              echo "⚠  No se pudo instalar requirements.txt (¿sin conexión?)."
              echo '   Vuelve a entrar con "nix develop" o corre: uv pip install -r requirements.txt'
            fi
          fi

          if [ ! -d "$HOME/.local/share/jupyter/kernels/kafka-lab" ]; then
            python -m ipykernel install --user --name=kafka-lab \
              --display-name "Python (kafka-lab .venv)" > /dev/null 2>&1 || true
          fi

          echo ""
          echo "╔══════════════════════════════════════════════════════════╗"
          echo "║  Laboratorio Kafka — Python (uv) + Docker/Podman         ║"
          echo "╚══════════════════════════════════════════════════════════╝"
          echo ""
          echo "Python:  $(python --version 2>&1)"
          echo "uv:      $(uv --version)"
          echo "kcat:    $(kcat -V 2>&1 | head -1)"
          echo ""
          echo "Comandos:"
          echo "  docker compose up -d            Levantar broker Kafka (alias de podman)"
          echo "  podman-compose up -d            Levantar broker Kafka (directo)"
          echo "  docker compose ps               Estado de los servicios"
          echo "  docker compose logs -f kafka    Logs del broker"
          echo "  docker compose exec kafka ...   Ejecutar Kafka CLI dentro del contenedor"
          echo "  kcat -L -b localhost:9092       Listar topics"
          echo "  python producer.py              Producer (Part-3 del lab)"
          echo "  python consumer.py              Consumer (Part-3 del lab)"
          echo "  python continuous_producer.py   Producer continuo (Mini Challenge)"
          echo "  python monitor_consumer.py      Monitor con metricas (Mini Challenge)"
          echo "  uv pip install <pkg>            Instalar paquete extra en .venv"
          echo "  requirements.txt                Dependencias Python (uv las instala al entrar)"
          echo "  pytest                          Correr tests (aun sin tests: no tests ran)"
          echo "  jupyter lab                     JupyterLab"
          echo ""
          echo "Guia completa: docs/commands.md   |   Lab: docs/ETL-G1_2026-2_U3-Activity-1_ApacheKafkaFlow.md"
          echo ""
        '';
      };
    };
}
