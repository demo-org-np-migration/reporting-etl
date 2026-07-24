# reporting-etl

Consumidor de `payments.completed` que carga el warehouse. Por cada pago liquidado escribe
una fila en `fact_payments` y actualiza el acumulado de `daily_volume`. Es la fuente para
cualquier reporte de volumen que armemos sobre pagos: nada de leer directo de `payments` en
la base transaccional.

## Historia

Este repo nació en el equipo de payments, como un script que Tomás escribía a demanda cada
vez que finanzas pedía un número de volumen. Cuando empezamos a necesitar el dato al día
(y no una corrida manual cada tanto) lo convertimos en un consumidor de Kafka de verdad, y
cuando Tomás no daba abasto con el resto de payments-api lo heredó platform. Yo (Paula) soy
la owner desde entonces; Martín Acosta ayuda con el chart y los workflows.

Es chiquito a propósito: una tabla de hechos y un acumulado diario, nada de agregaciones
pesadas ni de un data warehouse de verdad detrás. Si en algún momento necesitamos más
dimensiones (por merchant, por cuenta) lo vemos, pero por ahora esto alcanza para lo que
finanzas pide.

**Nota pendiente:** los `resources.requests` de prod se subieron en 2026-03 durante el
incidente INC-142 (lag de consumo que veníamos arrastrando) "por las dudas", y nunca los
volvimos a bajar. El consumo real medido en Grafana es ~30m de CPU y ~90Mi de memoria, muy
por debajo de lo que gitops tiene pedido para prod hoy. Quedó en el backlog de platform
revisarlo — el chart de este repo (`chart/values.yaml`) siempre tuvo el default sano; lo que
hay que tocar es `gitops/apps/reporting-etl/values-prod.yaml`.

## Cómo corre

1. Al arrancar aplica `sql/001_init.sql` contra `warehouse` (`CREATE TABLE IF NOT EXISTS`,
   así que es seguro correrlo en cada deploy).
2. Levanta un servidor HTTP mínimo en un thread aparte, puerto `8080`, con `/health` y
   `/metrics`.
3. Se suscribe a `<KAFKA_TOPIC_PREFIX>payments.completed` con el group `reporting-etl` y por
   cada evento: upsert en `fact_payments` (`ON CONFLICT (payment_id) DO NOTHING`) y, solo si
   el evento era nuevo, `INSERT ... ON CONFLICT (day, currency) DO UPDATE` en `daily_volume`
   sumando `amount` e incrementando `count`. Así un reintento o un replay del topic no duplica
   ni vuelve a acumular.

Elegimos `confluent-kafka` (sobre `aiokafka`) porque el resto del proceso es sincrónico
(`psycopg` sync, `http.server` en un thread) y nos evita mezclar asyncio con un solo consumer.

## Variables de entorno

| Variable | Ejemplo | Notas |
|---|---|---|
| `SERVICE_NAME` | `reporting-etl` | |
| `ENV` | `staging` \| `prod` | |
| `PORT` | `8080` | |
| `LOG_LEVEL` | `INFO` | |
| `KAFKA_BOOTSTRAP` | `kafka.platform.svc:9092` | |
| `KAFKA_TOPIC_PREFIX` | `staging.` | se concatena con `payments.completed` |
| `DB_HOST` | `postgres.staging.svc` | |
| `DB_NAME` | `warehouse` | |
| `DB_USER` / `DB_PASSWORD` | — | desde el secret `postgres-credentials` |

## Correr local

```
uv sync --group dev
DB_HOST=localhost DB_USER=cauri DB_PASSWORD=cauri KAFKA_BOOTSTRAP=localhost:9092 \
  uv run python -m etl.main
```

## Tests

```
uv run pytest -q
uv run ruff check .
```

`tests/test_loader.py` corre contra una conexión fake en memoria (sin Postgres real): valida
que un evento carga `fact_payments` y `daily_volume`, y que el mismo evento repetido no
duplica la fila ni vuelve a sumar al acumulado.

## Deploy

Estilo C (Argo CD + Helm, las convenciones internas de API §9). El chart vive en `chart/`; los `values-<env>.yaml`
reales están en el repo `gitops` (`apps/reporting-etl/`), no acá. `build.yml` buildea, pushea
a `ghcr.io/demo-org-np-migration/reporting-etl` y bumpea el tag de imagen en `gitops` (staging
en cada push a `main`, prod además en cada tag `v*`). Argo CD sincroniza desde ahí.

```
helm lint chart
helm template chart
```
