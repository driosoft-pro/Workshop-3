#!/usr/bin/env bash
# ==============================================================================
# run.sh — Script unificado de control y ejecución del Workshop 3
# Compatible con: NixOS, Linux, WSL, macOS
# Motores soportados: Podman (rootless) y Docker
# ==============================================================================
set -euo pipefail

# Directorios base
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

LOGS_DIR="$SCRIPT_DIR/logs"
mkdir -p "$LOGS_DIR"
TIMESTAMP="$(date +%Y%m%d_%H%M%S)"
LOG_FILE="$LOGS_DIR/run_${TIMESTAMP}.log"
LATEST_LOG="$LOGS_DIR/latest.log"

# Colores para la terminal
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
CYAN='\033[0;36m'
BOLD='\033[1m'
NC='\033[0m' # No Color

# Función de log dual (pantalla + archivo)
log() {
    local level="$1"
    local msg="$2"
    local color="$NC"
    case "$level" in
        "INFO")  color="$CYAN" ;;
        "STEP")  color="$BLUE$BOLD" ;;
        "OK")    color="$GREEN$BOLD" ;;
        "WARN")  color="$YELLOW" ;;
        "ERROR") color="$RED$BOLD" ;;
        "CMD")   color="$BOLD" ;;
    esac
    echo -e "${color}[$level]${NC} $msg" | tee -a "$LOG_FILE"
}

# Enlace al último log
ln -sf "$LOG_FILE" "$LATEST_LOG" 2>/dev/null || true

# ------------------------------------------------------------------------------
# Detección de herramientas
# ------------------------------------------------------------------------------
detect_compose() {
    # 1. Si existe Podman y socket rootless en NixOS, configurar socket
    if [ -S "/run/user/$(id -u)/podman/podman.sock" ]; then
        export CONTAINER_HOST="unix:///run/user/$(id -u)/podman/podman.sock"
        [ -z "${DOCKER_HOST:-}" ] && export DOCKER_HOST="unix:///run/user/$(id -u)/podman/podman.sock"
    fi

    if command -v podman-compose >/dev/null 2>&1; then
        COMPOSE_CMD="podman-compose"
        CONTAINER_CLI="podman"
    elif docker compose version >/dev/null 2>&1; then
        COMPOSE_CMD="docker compose"
        CONTAINER_CLI="docker"
    elif command -v docker-compose >/dev/null 2>&1; then
        COMPOSE_CMD="docker-compose"
        CONTAINER_CLI="docker"
    else
        log "ERROR" "No se encontró podman-compose ni docker compose en el sistema."
        exit 1
    fi
    log "INFO" "Motor de contenedores detectado: $COMPOSE_CMD ($CONTAINER_CLI)"
}

# Ejecutar comandos de Python dentro del entorno adecuado (Nix o venv directo)
run_py() {
    local cmd="$*"
    log "CMD" "python $cmd"
    if [ -n "${IN_NIX_SHELL:-}" ] || [ -f "/etc/NIXOS" ] && [ -n "${VIRTUAL_ENV:-}" ]; then
        python $cmd 2>&1 | tee -a "$LOG_FILE"
    elif command -v nix >/dev/null 2>&1 && [ -f "flake.nix" ]; then
        nix develop --command bash -c "python $cmd" 2>&1 | tee -a "$LOG_FILE"
    elif [ -f ".venv/bin/python" ]; then
        .venv/bin/python $cmd 2>&1 | tee -a "$LOG_FILE"
    else
        python3 $cmd 2>&1 | tee -a "$LOG_FILE"
    fi
}

run_pytest() {
    log "CMD" "pytest -q"
    if [ -n "${IN_NIX_SHELL:-}" ] || [ -f "/etc/NIXOS" ] && [ -n "${VIRTUAL_ENV:-}" ]; then
        pytest -q 2>&1 | tee -a "$LOG_FILE"
    elif command -v nix >/dev/null 2>&1 && [ -f "flake.nix" ]; then
        nix develop --command bash -c "pytest -q" 2>&1 | tee -a "$LOG_FILE"
    elif [ -f ".venv/bin/pytest" ]; then
        .venv/bin/pytest -q 2>&1 | tee -a "$LOG_FILE"
    else
        pytest -q 2>&1 | tee -a "$LOG_FILE"
    fi
}

# ------------------------------------------------------------------------------
# Consumer ML (proceso del host, expone /metrics en el puerto 8000)
# ------------------------------------------------------------------------------
CONSUMER_LOG="$LOGS_DIR/consumer.log"
CONSUMER_PID_FILE="$LOGS_DIR/consumer.pid"

# ¿El endpoint /metrics responde? (up | down)
consumer_metrics_up() {
    [ "$(http_state "http://localhost:${METRICS_PORT:-8000}/metrics")" = "up" ]
}

# Arranca kafka/consumer.py en segundo plano (idempotente) y espera a /metrics.
start_consumer() {
    if consumer_metrics_up; then
        log "OK" "Consumer ML ya está corriendo -> http://localhost:${METRICS_PORT:-8000}/metrics"
        return 0
    fi

    log "STEP" "Iniciando Consumer ML en el host (kafka/consumer.py)..."
    if [ -n "${IN_NIX_SHELL:-}" ] || { [ -f "/etc/NIXOS" ] && [ -n "${VIRTUAL_ENV:-}" ]; }; then
        nohup python kafka/consumer.py >> "$CONSUMER_LOG" 2>&1 </dev/null &
    elif command -v nix >/dev/null 2>&1 && [ -f "flake.nix" ]; then
        nohup nix develop --command python kafka/consumer.py >> "$CONSUMER_LOG" 2>&1 </dev/null &
    elif [ -f ".venv/bin/python" ]; then
        nohup .venv/bin/python kafka/consumer.py >> "$CONSUMER_LOG" 2>&1 </dev/null &
    else
        nohup python3 kafka/consumer.py >> "$CONSUMER_LOG" 2>&1 </dev/null &
    fi
    echo $! > "$CONSUMER_PID_FILE"

    local i
    for i in $(seq 1 15); do
        if consumer_metrics_up; then
            log "OK" "Consumer ML activo -> http://localhost:${METRICS_PORT:-8000}/metrics (log: ${CONSUMER_LOG#"$SCRIPT_DIR/"})"
            return 0
        fi
        sleep 1
    done
    log "WARN" "El consumer no abrió /metrics en 15s — revisa ${CONSUMER_LOG#"$SCRIPT_DIR/"}"
    tail -n 5 "$CONSUMER_LOG" 2>/dev/null | tee -a "$LOG_FILE" || true
    return 0
}

# Detiene el consumer del host (pid file + respaldo por patrón).
stop_consumer() {
    local pid=""
    if [ -f "$CONSUMER_PID_FILE" ]; then
        pid="$(cat "$CONSUMER_PID_FILE" 2>/dev/null || true)"
        if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then
            kill "$pid" 2>/dev/null || true
            log "INFO" "Consumer ML detenido (pid $pid)."
        fi
        rm -f "$CONSUMER_PID_FILE"
    fi
    if command -v pkill >/dev/null 2>&1; then
        pkill -f "[k]afka/consumer.py" 2>/dev/null || true
    fi
    return 0
}

# Reinicia el consumer (necesario tras reentrenar: cachea model.pkl en memoria).
restart_consumer() {
    stop_consumer
    sleep 1
    start_consumer
}

# ------------------------------------------------------------------------------
# Comandos de ciclo de vida de Infraestructura
# ------------------------------------------------------------------------------
infra_start() {
    detect_compose
    log "STEP" "Iniciando infraestructura base (Kafka + Postgres)..."
    $COMPOSE_CMD up -d kafka kafka-init postgres 2>&1 | tee -a "$LOG_FILE"

    log "INFO" "Esperando a que Postgres y Kafka reporten estado saludable (healthy)..."
    local kh="missing" ph="missing" i
    for i in $(seq 1 30); do
        kh=$($CONTAINER_CLI inspect -f '{{.State.Health.Status}}' kafka 2>/dev/null || echo missing)
        ph=$($CONTAINER_CLI inspect -f '{{.State.Health.Status}}' happiness-postgres 2>/dev/null || echo missing)
        if [ "$kh" = "healthy" ] && [ "$ph" = "healthy" ]; then
            log "OK" "Infraestructura base iniciada exitosamente (kafka + postgres healthy)."
            start_consumer
            return 0
        fi
        sleep 4
    done
    log "ERROR" "Infraestructura no saludable tras 120s: kafka=$kh postgres=$ph"
    log "ERROR" "Revisa: $CONTAINER_CLI logs kafka"
    return 1
}

infra_start_all() {
    detect_compose
    log "STEP" "Iniciando todos los servicios (Kafka, Postgres, Airflow, Superset, Prometheus, Grafana)..."
    $COMPOSE_CMD --profile monitoring --profile dashboards --profile airflow up -d --build 2>&1 | tee -a "$LOG_FILE"
    log "OK" "Todos los servicios fueron levantados."

    log "STEP" "Creando/actualizando el dashboard de Superset (Happiness KPIs)..."
    local i
    for i in $(seq 1 30); do
        curl -sf -o /dev/null -m 5 http://localhost:8088/login/ 2>/dev/null && break
        sleep 4
    done
    if run_py "dashboards/superset/bootstrap_dashboards.py"; then
        log "OK" "Dashboard 'Happiness KPIs' -> http://localhost:8088/superset/dashboard/1/"
    else
        log "WARN" "No se pudo crear el dashboard de Superset (máncalo a mano: python dashboards/superset/bootstrap_dashboards.py)"
    fi

    # Prometheus scrapea host.containers.internal:8000 → sin consumer no hay series en Grafana
    start_consumer
}

infra_stop() {
    detect_compose
    stop_consumer
    log "STEP" "Deteniendo todos los contenedores..."
    $COMPOSE_CMD --profile monitoring --profile dashboards --profile airflow stop 2>&1 | tee -a "$LOG_FILE"
    log "OK" "Contenedores detenidos."
}

infra_clean() {
    detect_compose
    stop_consumer
    log "WARN" "Eliminando contenedores, redes y volúmenes persistentes..."
    $COMPOSE_CMD --profile monitoring --profile dashboards --profile airflow down -v --remove-orphans 2>&1 | tee -a "$LOG_FILE"
    log "OK" "Infraestructura y datos persistentes eliminados."
}

infra_recreate() {
    detect_compose
    log "STEP" "Recreando proyecto desde cero..."
    infra_clean
    infra_start
    log "STEP" "Ejecutando pipeline batch inicial para cargar esquema y modelos..."
    run_py "-m src.harmonize"
    run_py "-m src.train"
    log "OK" "Proyecto recreado e inicializado exitosamente."
}

infra_status() {
    detect_compose
    log "STEP" "Estado actual de los contenedores:"
    if [ "$CONTAINER_CLI" = "podman" ] && podman --remote ps >/dev/null 2>&1; then
        podman --remote ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}" | tee -a "$LOG_FILE"
    else
        $COMPOSE_CMD ps | tee -a "$LOG_FILE"
    fi

    echo ""
    log "STEP" "Estado de tablas en PostgreSQL:"
    local SQL_QUERY="SELECT processing_status, count(*) FROM raw_happiness_events GROUP BY processing_status; SELECT count(*) AS fact_predictions_count FROM fact_predictions;"
    if command -v psql >/dev/null 2>&1; then
        PGPASSWORD=happiness psql -h localhost -U happiness -d happiness -c "$SQL_QUERY" 2>&1 | tee -a "$LOG_FILE" || log "WARN" "Base de datos aún sin inicializar."
    elif command -v nix >/dev/null 2>&1; then
        nix develop --command bash -c "PGPASSWORD=happiness psql -h localhost -U happiness -d happiness -c '$SQL_QUERY'" 2>&1 | tee -a "$LOG_FILE" || log "WARN" "Base de datos aún sin inicializar."
    fi
}

# ------------------------------------------------------------------------------
# Enlaces de aplicaciones y estado detallado
# ------------------------------------------------------------------------------
# Estado de un contenedor: running | exited | missing
resolve_container() {
    local name matches
    name="$($CONTAINER_CLI inspect -f '{{.Name}}' "$1" 2>/dev/null)" || name=""
    if [ -z "$name" ]; then
        # podman-compose nombra contenedores <proyecto>_<servicio>_1 -> resolver por label
        # (puede haber contenedores viejos de otros proyectos: preferir los running)
        matches="$($CONTAINER_CLI ps -a --filter "label=com.docker.compose.service=$1" \
            --format '{{.Names}} {{.State}}' 2>/dev/null)"
        name="$(echo "$matches" | awk '$2=="running"{print $1; exit}')"
        [ -z "$name" ] && name="$(echo "$matches" | head -1 | awk '{print $1}')"
    fi
    echo "${name#/}"
}

container_state() {
    local name st
    name="$(resolve_container "$1")"
    if [ -z "$name" ]; then echo "missing"; return 0; fi
    st="$($CONTAINER_CLI inspect -f '{{.State.Status}}' "$name" 2>/dev/null)" || st="missing"
    echo "${st:-missing}"
}

# Estado de una URL: up | down
http_state() {
    if curl -sf -o /dev/null -m 3 "$1" 2>/dev/null; then echo "up"; else echo "down"; fi
}

# Opción 12: enlaces de todas las aplicaciones + si el proyecto está en ejecución
show_links() {
    detect_compose
    echo ""
    local ks ps
    ks="$(container_state kafka)"
    ps="$(container_state happiness-postgres)"

    if [ "$ks" = "running" ] && [ "$ps" = "running" ]; then
        log "OK" "PROYECTO EN EJECUCIÓN — enlaces de las aplicaciones:"
    else
        log "WARN" "PROYECTO NO EN EJECUCIÓN (kafka=$ks, postgres=$ps)."
        log "INFO" "Ejecuta primero el proyecto: opción 1 (automática) u opción 4 (start-all)."
    fi
    echo ""

    local -a apps=(
        "Airflow|http://localhost:8080|airflow-webserver|admin/admin — orquestador batch (DAGs)"
        "Airflow (dashboard)|http://localhost:8080/home|airflow-webserver|demo éxito: opción 9 · fallo: opción 10"
        "Superset|http://localhost:8088|superset|admin/admin — dashboard 'Happiness KPIs'"
        "Grafana|http://localhost:3000|grafana|admin/admin — salud del pipeline"
        "Prometheus|http://localhost:9090|prometheus|sin login — métricas del consumer"
        "Kafka (bootstrap)|localhost:29092|kafka|topic happiness-events (3 particiones)"
        "PostgreSQL|localhost:5432|happiness-postgres|happiness/happiness — base happiness"
        "Consumer ML /metrics|http://localhost:8000/metrics||corre en el host — lo levanta la opción 3"
    )

    printf "  %-24s %-36s %-10s %s\n" "APLICACIÓN" "URL" "ESTADO" "NOTAS"
    printf "  %-24s %-36s %-10s %s\n" "------------------------" "------------------------------------" "----------" "-----"
    local row name url ctr note state color
    for row in "${apps[@]}"; do
        IFS='|' read -r name url ctr note <<< "$row"
        if [ -n "$ctr" ]; then
            if [ "$(container_state "$ctr")" = "running" ]; then state="ACTIVO"; color="$GREEN"; else state="DETENIDO"; color="$RED"; fi
        else
            if [ "$(http_state "$url")" = "up" ]; then state="ACTIVO"; color="$GREEN"; else state="DETENIDO"; color="$RED"; fi
        fi
        printf "  %-24s %-36s ${color}%-10s${NC} %s\n" "$name" "$url" "$state" "$note"
    done
    echo ""
    log "INFO" "Airflow / Superset / Grafana / Prometheus se levantan con la opción 4 (start-all)."
}

# Opción 13: estado detallado en terminal (contenedores, Kafka, modelo, apps)
show_detail() {
    detect_compose
    echo ""

    log "STEP" "1) Contenedores clave (estado / salud / reinicios):"
    printf "  %-20s %-10s %-14s %s\n" "CONTENEDOR" "ESTADO" "SALUD" "REINICIOS"
    local c st hc rc rn
    for c in kafka happiness-postgres kafka-init airflow-webserver airflow-scheduler superset grafana prometheus; do
        st="$(container_state "$c")"
        if [ "$st" = "missing" ]; then
            printf "  %-20s %-10s %-14s %s\n" "$c" "-" "-" "no creado"
            continue
        fi
        rn="$(resolve_container "$c")"
        hc="$($CONTAINER_CLI inspect -f '{{if .State.Health}}{{.State.Health.Status}}{{else}}sin-healthcheck{{end}}' "$rn" 2>/dev/null || echo "?")"
        rc="$($CONTAINER_CLI inspect -f '{{.RestartCount}}' "$rn" 2>/dev/null || echo "?")"
        printf "  %-20s %-10s %-14s %s\n" "$c" "$st" "$hc" "$rc"
    done

    echo ""
    if [ "$(container_state kafka)" = "running" ]; then
        log "STEP" "2) Kafka — topic happiness-events:"
        $CONTAINER_CLI exec kafka /opt/kafka/bin/kafka-topics.sh \
            --bootstrap-server localhost:9092 --describe --topic happiness-events 2>&1 | tee -a "$LOG_FILE" \
            || log "WARN" "No se pudo describir el topic."
        log "STEP" "3) Kafka — grupo de consumidores happiness-ml-team:"
        $CONTAINER_CLI exec kafka /opt/kafka/bin/kafka-consumer-groups.sh \
            --bootstrap-server localhost:9092 --describe --group happiness-ml-team 2>&1 | tee -a "$LOG_FILE" \
            || log "WARN" "Grupo sin offsets todavía (el consumer aún no ha corrido)."
    else
        log "WARN" "Kafka detenido — no se puede consultar el topic (ejecuta la opción 3)."
    fi

    echo ""
    log "STEP" "4) Modelo ML:"
    if [ -f "models/model.pkl" ]; then
        ls -lh models/model.pkl | tee -a "$LOG_FILE"
        grep -E '"(algorithm|model_version|mae|rmse|r2|n_rows|trained_at)"' models/model_metadata.json 2>/dev/null | tee -a "$LOG_FILE" || true
    else
        log "WARN" "models/model.pkl no existe — entrena con: python -m src.train (o ./run.sh recreate)"
    fi

    echo ""
    log "STEP" "5) Consumer ML (corre en el host):"
    if [ "$(http_state http://localhost:8000/metrics)" = "up" ]; then
        log "OK" "Métricas activas -> http://localhost:8000/metrics"
    else
        log "WARN" "Consumer no está corriendo — arráncalo con: python kafka/consumer.py"
    fi

    echo ""
    log "STEP" "6) Uso de las aplicaciones en la terminal:"
    {
        echo "  Airflow     : curl -s localhost:8080/health"
        echo "                $CONTAINER_CLI exec $(resolve_container airflow-scheduler) airflow dags list"
        echo "  Superset    : curl -s -o /dev/null -w '%{http_code}\n' localhost:8088/login/"
        echo "  Grafana     : curl -s localhost:3000/api/health"
        echo "  Prometheus  : curl -s localhost:9090/api/v1/targets | head -c 300"
        echo "  Kafka       : $CONTAINER_CLI exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list"
        echo "  PostgreSQL  : PGPASSWORD=happiness psql -h localhost -U happiness -d happiness -c 'SELECT processing_status, count(*) FROM raw_happiness_events GROUP BY 1;'"
        echo "  Consumer ML : curl -s localhost:8000/metrics | head"
        echo "  Modelo      : python -c \"import joblib; m=joblib.load('models/model.pkl'); print(m)\""
    } | tee -a "$LOG_FILE"
}

# ------------------------------------------------------------------------------
# Ejecuciones de Airflow
# ------------------------------------------------------------------------------
run_airflow_success() {
    detect_compose
    log "STEP" "Ejecutando Airflow DAG de Éxito: happiness_batch_training..."
    local sched_id=""
    sched_id="$($CONTAINER_CLI ps -q -f name=airflow-scheduler | head -n 1)"
    if [ -z "$sched_id" ] && [ "$CONTAINER_CLI" = "podman" ]; then
        sched_id="$(podman --remote ps -q -f name=airflow-scheduler | head -n 1 || true)"
    fi

    if [ -z "$sched_id" ]; then
        log "INFO" "Levantando servicio Airflow..."
        $COMPOSE_CMD --profile airflow up -d --build
        sleep 5
        sched_id="$($CONTAINER_CLI ps -q -f name=airflow-scheduler | head -n 1)"
    fi

    log "CMD" "$CONTAINER_CLI exec $sched_id airflow dags test happiness_batch_training"
    if [ "$CONTAINER_CLI" = "podman" ] && podman --remote ps >/dev/null 2>&1; then
        podman --remote exec "$sched_id" airflow dags test happiness_batch_training 2>&1 | tee -a "$LOG_FILE"
    else
        $CONTAINER_CLI exec "$sched_id" airflow dags test happiness_batch_training 2>&1 | tee -a "$LOG_FILE"
    fi
    log "OK" "Airflow DAG de Éxito completado."
}

run_airflow_failure() {
    detect_compose
    log "STEP" "Ejecutando Airflow DAG con Error Controlado: happiness_batch_failure_demo..."
    local sched_id=""
    sched_id="$($CONTAINER_CLI ps -q -f name=airflow-scheduler | head -n 1)"
    if [ -z "$sched_id" ] && [ "$CONTAINER_CLI" = "podman" ]; then
        sched_id="$(podman --remote ps -q -f name=airflow-scheduler | head -n 1 || true)"
    fi

    if [ -z "$sched_id" ]; then
        log "INFO" "Levantando servicio Airflow..."
        $COMPOSE_CMD --profile airflow up -d --build
        sleep 5
        sched_id="$($CONTAINER_CLI ps -q -f name=airflow-scheduler | head -n 1)"
    fi

    log "INFO" "Este DAG inyecta un valor anómalo en validate_ge para demostrar el aislamiento de error en Airflow."
    set +e
    if [ "$CONTAINER_CLI" = "podman" ] && podman --remote ps >/dev/null 2>&1; then
        podman --remote exec "$sched_id" airflow dags test happiness_batch_failure_demo 2>&1 | tee -a "$LOG_FILE"
    else
        $CONTAINER_CLI exec "$sched_id" airflow dags test happiness_batch_failure_demo 2>&1 | tee -a "$LOG_FILE"
    fi
    local exit_code=$?
    set -e
    log "OK" "Demostración de fallo controlado ejecutada. Airflow detuvo downstream y protegió el modelo (código de salida: $exit_code)."
}

# ------------------------------------------------------------------------------
# Flujo Automático Completo
# ------------------------------------------------------------------------------
run_auto() {
    log "STEP" "=========================================================="
    log "STEP" "Iniciando Ejecución Automática Completa del Proyecto"
    log "STEP" "Registro guardándose en: $LOG_FILE"
    log "STEP" "=========================================================="

    # 1. Infra
    infra_start

    # 2. Batch ETL
    log "STEP" "Paso 1: Armonización de datos (5 CSVs 2015-2019)..."
    run_py "-m src.harmonize"

    log "STEP" "Paso 2: Validación con Great Expectations..."
    run_py "-m src.validate"

    log "STEP" "Paso 3: Entrenamiento del modelo (GroupShuffleSplit + LinearRegression)..."
    run_py "-m src.train"

    # 3. Tests
    log "STEP" "Paso 4: Ejecución de la suite de pruebas del contrato (pytest)..."
    run_pytest

    # 4. Streaming Simulado
    log "STEP" "Paso 5: Transmisión Streaming Kafka (Consumidor + Productor con inválidos)..."
    # El modelo cambió en el Paso 3 y el consumer cachea model.pkl en memoria
    log "INFO" "Reiniciando consumidor para cargar el modelo recién entrenado..."
    restart_consumer

    log "INFO" "Iniciando productor emitiendo eventos de prueba y eventos inválidos de demo..."
    run_py "kafka/producer.py --delay 0.05 --run-id auto_demo --inject-invalid"

    # Dar tiempo a que el consumidor procese los últimos eventos y commitee
    sleep 4

    # 5. Dashboard KPIs
    log "STEP" "Paso 6: Actualización de gráficos de KPIs desde PostgreSQL..."
    run_py "dashboards/kpi_charts.py"

    log "STEP" "Paso 7: Verificación final de estado:"
    infra_status

    log "OK" "=========================================================="
    log "OK" "¡Ejecución Automática Finalizada Exitosamente!"
    log "OK" "Consulte el log detallado en: $LOG_FILE"
    log "OK" "=========================================================="
}

# ------------------------------------------------------------------------------
# Flujo Paso a Paso Interactivo
# ------------------------------------------------------------------------------
confirm_step() {
    local prompt="$1"
    echo -e -n "\n${YELLOW}${BOLD}[CONFIRMAR]${NC} ¿Desea ejecutar: ${BOLD}$prompt${NC}? [S/n]: "
    read -r resp
    case "$resp" in
        [nN][oO]|[nN]) return 1 ;;
        *) return 0 ;;
    esac
}

run_step_by_step() {
    log "STEP" "=========================================================="
    log "STEP" "Modo Paso a Paso Interactivo"
    log "STEP" "=========================================================="

    if confirm_step "1. Iniciar Infraestructura (Kafka + Postgres)"; then
        infra_start
    fi

    if confirm_step "2. Armonizar CSVs (src.harmonize)"; then
        run_py "-m src.harmonize"
    fi

    if confirm_step "3. Validar calidad de datos con Great Expectations (src.validate)"; then
        run_py "-m src.validate"
    fi

    if confirm_step "4. Entrenar modelo y exportar datos streaming (src.train)"; then
        run_py "-m src.train"
    fi

    if confirm_step "5. Ejecutar suite de pruebas de contrato (pytest)"; then
        run_pytest
    fi

    if confirm_step "6. Demostración Streaming (Productor + Consumidor)"; then
        log "INFO" "Asegurando consumidor en segundo plano (queda activo tras la demo)..."
        restart_consumer
        run_py "kafka/producer.py --delay 0.05 --run-id step_demo --inject-invalid"
        sleep 3
    fi

    if confirm_step "7. Regenerar gráficos de KPIs (dashboards/kpi_charts.py)"; then
        run_py "dashboards/kpi_charts.py"
    fi

    if confirm_step "8. Consultar estado en base de datos"; then
        infra_status
    fi

    log "OK" "Modo Paso a Paso concluido."
}

# ------------------------------------------------------------------------------
# Menú Interactivo
# ------------------------------------------------------------------------------
show_menu() {
    clear 2>/dev/null || true
    echo -e "${CYAN}${BOLD}╔══════════════════════════════════════════════════════════════════╗${NC}"
    echo -e "${CYAN}${BOLD}║      Workshop 3 — Streaming ETL (Kafka + ML + PostgreSQL)        ║${NC}"
    echo -e "${CYAN}${BOLD}╚══════════════════════════════════════════════════════════════════╝${NC}"
    echo ""
    echo -e "  ${BOLD}1)${NC} Ejecución Automática Completa (Pipeline End-to-End)"
    echo -e "  ${BOLD}2)${NC} Ejecución Paso a Paso Interactiva"
    echo -e "  ${BOLD}3)${NC} Iniciar Infraestructura Base (Kafka + PostgreSQL)"
    echo -e "  ${BOLD}4)${NC} Iniciar Todos los Servicios (Airflow, Superset, Grafana, Prom)"
    echo -e "  ${BOLD}5)${NC} Detener Contenedores (stop)"
    echo -e "  ${BOLD}6)${NC} Eliminar Contenedores y Volúmenes (clean / reset)"
    echo -e "  ${BOLD}7)${NC} Recrear Proyecto desde cero"
    echo -e "  ${BOLD}8)${NC} Ejecutar Tests de Contrato (pytest)"
    echo -e "  ${BOLD}9)${NC} Airflow DAG: Demo Éxito (happiness_batch_training)"
    echo -e "  ${BOLD}10)${NC} Airflow DAG: Demo Error Controlado (happiness_batch_failure_demo)"
    echo -e "  ${BOLD}11)${NC} Ver Estado Actual (Contenedores + Base de Datos)"
    echo -e "  ${BOLD}12)${NC} Ver Enlaces de las Aplicaciones (Airflow, Superset, Grafana, Prom...)"
    echo -e "  ${BOLD}13)${NC} Estado Detallado (Contenedores, Kafka, Modelo, Apps)"
    echo -e "  ${BOLD}0)${NC} Salir"
    echo ""
    echo -n "Seleccione una opción [0-13]: "
    read -r opt
    case "$opt" in
        1) run_auto ;;
        2) run_step_by_step ;;
        3) infra_start ;;
        4) infra_start_all ;;
        5) infra_stop ;;
        6) infra_clean ;;
        7) infra_recreate ;;
        8) run_pytest ;;
        9) run_airflow_success ;;
        10) run_airflow_failure ;;
        11) infra_status ;;
        12) show_links ;;
        13) show_detail ;;
        0) exit 0 ;;
        *) log "WARN" "Opción inválida"; sleep 1; show_menu ;;
    esac
}

# ------------------------------------------------------------------------------
# Entrypoint CLI
# ------------------------------------------------------------------------------
MODE="${1:-}"
case "$MODE" in
    "auto")             run_auto ;;
    "step")             run_step_by_step ;;
    "start")            infra_start ;;
    "start-all")        infra_start_all ;;
    "stop")             infra_stop ;;
    "clean")            infra_clean ;;
    "recreate")         infra_recreate ;;
    "test")             run_pytest ;;
    "airflow-success")  run_airflow_success ;;
    "airflow-fail")     run_airflow_failure ;;
    "status")           infra_status ;;
    "links")            show_links ;;
    "detail")           show_detail ;;
    "help"|"-h"|"--help")
        echo "Uso: ./run.sh [auto|step|start|start-all|stop|clean|recreate|test|airflow-success|airflow-fail|status|links|detail]"
        echo "Sin argumentos abre el menú interactivo."
        ;;
    "")                 show_menu ;;
    *)
        log "ERROR" "Comando desconocido: $MODE. Use ./run.sh --help"
        exit 1
        ;;
esac
