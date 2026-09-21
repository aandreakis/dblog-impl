

# Una Implementación de Referencia de DBLog

[![CI](https://github.com/aandreakis/dblog-impl/actions/workflows/ci.yml/badge.svg)](https://github.com/aandreakis/dblog-impl/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![Java 21](https://img.shields.io/badge/Java-21-orange.svg)](https://openjdk.org/projects/jdk/21/)

Una implementación de referencia de **DBLog**, el algoritmo de captura de datos de cambio (CDC) basado en marcas de agua (*watermarks*) para actualizar el estado de una tabla mientras continúa la captura del registro de transacciones.

Este repositorio es publicado por uno de los coautores del artículo original de DBLog y la publicación en el blog de tecnología de Netflix. Su propósito es hacer que el algoritmo publicado sea ejecutable, inspeccionable y fácil de probar a partir de material público, para que los lectores puedan estudiar DBLog y comprender cómo funciona el algoritmo de marcas de agua.

[![Hydroscope - visualizador del algoritmo DBLog](ops/tap-tui/docs/img/hero.gif)](ops/tap-tui/README.md)

*DBLog en acción: el algoritmo de marcas de agua fusiona los cambios del registro de transacciones con lecturas por lotes acotados en un único flujo ordenado y limpio. Se muestra en la interfaz TUI de Hydroscope.*

## Por qué existe este repositorio

DBLog responde a una pregunta práctica sobre CDC: **¿cómo puede un sistema copiar filas de una tabla en lotes acotados mientras los cambios en vivo continúan llegando desde el registro de la base de datos?**

Este repositorio es útil si deseas:

- estudiar el algoritmo de marcas de agua de DBLog en código,
- ejecutar entornos de prueba de MySQL/PostgreSQL de extremo a extremo,
- auditar el artículo contra un comportamiento ejecutable,
- inspeccionar el comportamiento de reinicio, recuperación y puntos de control,
- utilizar una implementación de referencia compacta como punto de enseñanza o comparación.

Para auditores, [docs/PAPER_MAP.md](docs/PAPER_MAP.md) mapea cada paso del Algoritmo 1 al código, controles de cierre por fallo (*fail-closed*) y bloqueos de prueba, y rastrea explícitamente las variaciones con el artículo (modernizaciones y omisiones deliberadas).

Este repositorio no se recomienda para uso en producción. Para CDC en producción, utiliza un sistema mantenido como [Debezium](https://debezium.io/).

## DBLog en un minuto

Una instantánea por lotes puede entrar en carrera con eventos en vivo del registro. DBLog hace que esa carrera sea explícita y determinista:

1. continuar consumiendo transacciones confirmadas del registro de origen;
2. escribir una fila de **marca de agua baja** en la tabla de metadatos del origen;
3. leer un fragmento acotado por clave principal;
4. escribir una fila de **marca de agua alta**;
5. mientras el registro avanza de baja a alta, hacer pasar los eventos del registro y eliminar cualquier fila del lote seleccionado cuya clave principal haya sido modificada por un evento del registro más reciente;
6. cuando la marca de agua alta aparezca en el flujo del registro, emitir las filas restantes del lote y persistir el progreso del lote completado antes de confirmar el punto de control del origen.

La idea clave es que las filas de la instantánea son provisionales, mientras que los eventos del registro dentro de la ventana son más recientes y ganan en caso de colisión. Para el algoritmo formal y su motivación, lee el [artículo](https://arxiv.org/abs/2010.12597) y la [publicación en el blog de tecnología de Netflix](https://netflixtechblog.com/dblog-a-generic-change-data-capture-framework-69351fb9099b). Para un mapa de auditoría de artículo a código, consulta [docs/PAPER_MAP.md](docs/PAPER_MAP.md).

<p align="center">
  <img src="docs/img/dblog-one-minute.png" alt="DBLog watermark-window reconciliation" width="920">
</p>

<p align="center"><em>En este ejemplo, el lote selecciona pk=41, pk=42 y pk=43. La entrada del lote para pk=42 se descarta porque se recibe un evento UPDATE para esa clave en el LOG dentro de la ventana observada LOW–HIGH. La vía OUTPUT muestra el flujo reconciliado después de HIGH: eventos del registro dentro de la ventana más las filas del lote que sobrevivieron.</em></p>

## Inicio rápido

Prerrequisitos:

- Java 21
- Docker, para demostraciones y pruebas respaldadas por Docker
- Python 3.9+, para `scripts/demo/*.py`
- Rust estable, solo si deseas compilar Hydroscope

Ejecuta la demostración más corta de extremo a extremo:

```bash
python3 scripts/demo/mysql_to_postgres.py
```

En Windows:

```powershell
py -3 scripts/demo/mysql_to_postgres.py
```

La demostración inicia entornos de prueba locales desechables, ejecuta DBLog en el host, envía un volcado `ALL_TABLES` a través del plano de control HTTP local, verifica la copia inicial, aplica cambios en vivo en el origen y verifica la convergencia nuevamente. Los registros se escriben bajo `build/demo/<demo-name>/runtime.log`. Los contenedores aislados de entornos de prueba de la demostración se detienen al salir; establece `DBLOG_DEMO_KEEP_CONTAINERS=1` para dejarlos en ejecución inspección.

En una caché fría, la primera ejecución descarga las imágenes de Docker y puede tardar unos minutos; las ejecuciones posteriores son marcadamente más rápidas. En caso de éxito, esta demostración imprime `Initial dump converged.`, luego `Live changes converged.`, y termina con `Demo succeeded.` con código de salida 0.

Comandos útiles de verificación:

```bash
./gradlew test                  # pruebas unitarias rápidas
./gradlew integrationTest       # pruebas de integración de adaptadores y estado
./gradlew integrationTestDocker # pruebas de integración de adaptadores respaldadas por Docker
./gradlew e2eTest               # escenarios de recuperación/drift/fallo en modo de inspección
./gradlew e2eTestDocker         # escenarios de convergencia y reparación en vivo con Docker
./gradlew compatibilityMatrix   # mysql:8.0/8.4/9.6 y postgres:14-18
```

Los tiempos de ejecución varían según el hardware y el estado de la caché de Docker: `test` finaliza en mucho menos de un minuto, `integrationTest` y `e2eTest` suelen tardar unos minutos, y las vías respaldadas por Docker (`integrationTestDocker`, `e2eTestDocker`) son más largas porque inician entornos de bases de datos reales. `compatibilityMatrix` es la más lenta por diseño: recorre la matriz completa de imágenes de origen y puede tardar más de 20 minutos.

Las credenciales de los entornos de prueba son desechables y vinculan los puertos de la base de datos a `127.0.0.1`. No las expongas en una red no confiable.

## Qué incluye

| Área | Incluido |
| --- | --- |
| Runtime | Java 21, Spring Boot, Gradle |
| Fuentes | Transmisión de binlog de MySQL; replicación lógica `pgoutput` de PostgreSQL |
| Matriz de imágenes de origen | `mysql:8.0`, `mysql:8.4`, `mysql:9.6`; `postgres:14` hasta `postgres:18` |
| Destinos (sinks) | Flujo/archivo NDJSON, destino de inspección H2 tipado, aplicación JDBC para PostgreSQL/MySQL, no-op explícito |
| Estado | Puntos de control H2 integrados, esquemas, solicitudes y progreso por lote |
| Plano de control | API HTTP local para estado, métricas, envío de solicitudes e inspección |
| Verificación | Pruebas unitarias, de integración, e2e respaldadas por Docker y de matriz de versiones de origen |
| Visualización | TUI de Rust Hydroscope opcional sobre un flujo de inspección (tap) educativo |

## Ruta de lectura

| Comienza aquí | Por qué |
| --- | --- |
| [docs/PAPER_MAP.md](docs/PAPER_MAP.md) | Cada paso del Algoritmo 1 mapeado al código, controles de cierre por fallo y bloqueos de prueba; variaciones con el artículo rastreadas |
| [WindowReconciler.java](src/main/java/io/github/aandreakis/dblog/core/reconcile/WindowReconciler.java) | Máquina de estados de marcas de agua baja/alta y manejo de colisiones |
| [DefaultDumpWindowCoordinator.java](src/main/java/io/github/aandreakis/dblog/core/request/DefaultDumpWindowCoordinator.java) | Abre ventanas de lotes y persiste límites de reinicio |
| [DefaultTargetedRepairCoordinator.java](src/main/java/io/github/aandreakis/dblog/core/request/DefaultTargetedRepairCoordinator.java) | Reparación dirigida por clave principal a través de la misma maquinaria de ventanas |
| [RuntimeRequestPump.java](src/main/java/io/github/aandreakis/dblog/runtime/loop/RuntimeRequestPump.java) | Intercala transmisión en vivo con solicitudes de operadores en cola |
| [docs/OPERATION.md](docs/OPERATION.md) | Configuración, modos de arranque, destinos y comportamiento operativo |
| [docs/CONTROL_PLANE.md](docs/CONTROL_PLANE.md) | Forma de la API HTTP y ciclo de vida de solicitudes |
| [docs/adapters/mysql.md](docs/adapters/mysql.md) / [docs/adapters/postgres.md](docs/adapters/postgres.md) | Precondiciones y límites específicos de la fuente |

Los agentes de codificación también deben leer [AGENTS.md](AGENTS.md).

## Ejecutar localmente

Las demostraciones de Python son el camino más rápido:

```bash
python3 scripts/demo/mysql_to_postgres.py
python3 scripts/demo/mysql_to_ndjson.py
python3 scripts/demo/postgres_to_mysql.py
```

Si las demostraciones reutilizan un stack de bases de datos que iniciaste manualmente con
`docker compose -f ops/docker/compose.yml up -d`, dejarán ese stack en ejecución.
Deténlo con `docker compose -f ops/docker/compose.yml down -v`.

Para ejecutar el runtime manualmente contra los entornos de prueba de Docker:

```bash
docker compose -f ops/docker/compose.yml up -d

./gradlew bootRun \
  --args="--spring.config.additional-location=file:./ops/docker/examples/local/mysql-to-postgres/application.properties"
```

Con el plano de control local habilitado:

```bash
curl -sS http://127.0.0.1:8085/api/v1/runtime/status

curl -sS -X POST http://127.0.0.1:8085/api/v1/requests \
  -H 'Content-Type: application/json' \
  -d '{"scope":"ALL_TABLES"}'
```

Si `8085` está en uso, pasa `--dblog.control-plane.port=<port>` a `bootRun` (o establece
`DBLOG_CONTROL_PLANE_PORT=<port>` para las demostraciones de Python, que de lo contrario
seleccionan automáticamente un puerto libre y lo reportan en stderr). El modo en vivo de Hydroscope entonces
necesita `--url http://127.0.0.1:<port>/api/v1/tap/stream` para coincidir.

Para ejemplos empaquetados en Docker y detalles de reinicio de entornos, consulta
[ops/docker/README.md](ops/docker/README.md).

## Límites intencionales

Estas son decisiones de alcance, no lagunas en la hoja de ruta:

- proceso único, host único; sin HA, elección de líder, arrendamientos (*leases*) ni protocolo de toma de control;
- solo plano de control local de envío/consulta; sin puntos finales de pausa, reanudación o cancelación;
- solo estado H2 integrado; sin almacén de estado distribuido;
- solo fuentes MySQL y PostgreSQL;
- solo destinos NDJSON, inspección H2, aplicación JDBC y no-op;
- manejo conservador de esquemas; sin flujo de evolución de esquema en línea, reproducción DDL
  ni tema de historial de esquemas;
- entrega al menos una vez; la aplicación JDBC es idempotente mediante upsert/eliminar por clave principal,
  mientras que los consumidores NDJSON deben deduplicar.

Las solicitudes de funciones, expansión amplia de adaptadores/destinos, trabajo de HA, peticiones de hoja de ruta y solicitudes de soporte están fuera del alcance. Pueden considerarse informes concretos de errores y pull requests pequeños de corrección de errores dentro del alcance existente; consulta
[CONTRIBUTING.md](CONTRIBUTING.md). Se agradecen los bifurcaciones (*forks*) y modificaciones privadas bajo la licencia.

## Hydroscope

Hydroscope es un visualizador autocontenido en Rust/ratatui del algoritmo de marcas de agua de DBLog. Una vez compilado, los escenarios de demostración incluidos reproducen ventanas deterministas sin necesidad de base de datos, Java o Docker, por lo que puedes observar paso a paso las lecturas por lotes, los eventos del registro dentro de la ventana y los descartos de filas impulsados por colisiones:

```bash
cd ops/tap-tui
cargo build --release --bins
./target/release/hydroscope --scenario showcase
```

El mismo binario se conecta a un proceso DBLog en vivo cuando se inicia sin una bandera de escenario (`./target/release/hydroscope`); consulta
[ops/tap-tui/README.md](ops/tap-tui/README.md) para la configuración del modo en vivo y el
[recorrido por Hydroscope](https://aandreakis.github.io/dblog-impl/ops/tap-tui/docs/)
para capturas de pantalla comentadas.

El flujo de inspección (*tap*) está desactivado intencionalmente de forma predeterminada y es solo para fines educativos. Un suscriptor lento puede bloquear el hilo de la bomba DBLog por diseño. Consulta
[docs/CONTROL_PLANE.md](docs/CONTROL_PLANE.md#54-educational-tap).

## Referencias

[DBLog: A Watermark Based Change-Data-Capture Framework](https://arxiv.org/abs/2010.12597)  
Andreas Andreakis and Ioannis Papapanagiotou · arXiv · 2020

[DBLog: A Generic Change-Data-Capture Framework](https://netflixtechblog.com/dblog-a-generic-change-data-capture-framework-69351fb9099b)  
Andreas Andreakis and Ioannis Papapanagiotou · Netflix Technology Blog · 2019

## Política de mantenimiento

Este repositorio se publica para estudio, verificación y experimentación: intencionalmente de bajo mantenimiento y con funciones estables. Pueden aceptarse correcciones de errores. Las solicitudes de funciones no se atenderán, y el proyecto no proporciona soporte ni una hoja de ruta pública.

## Aviso

Esta es una implementación de referencia independiente construida a partir de material público. No es el DBLog de producción de Netflix, no contiene código de producción de Netflix y no está afiliado, respaldado ni mantenido por Netflix. Consulta
[NOTICE](NOTICE) para notas de procedencia.

## Licencia

Publicada bajo la Licencia MIT. Consulta [LICENSE](LICENSE) y [NOTICE](NOTICE).
El contexto de patentes de terceros se indica con fines informativos en [PATENTS.md](PATENTS.md).
