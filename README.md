# SmartStock · API Gateway

Backend for Frontend de SmartStock. Expone una API GraphQL para la interfaz, reúne información de los tres microservicios y adapta sus protocolos internos a las consultas y acciones del operador.

**Stack** · TypeScript, NestJS, Apollo Server, gRPC, Kafka y PostgreSQL.

## Responsabilidades

- Exponer a la interfaz la consulta de stock, el registro de ventas y la consulta de su estado.
- Componer respuestas que reúnen datos de más de un servicio y traducir errores a respuestas que pueda manejar el frontend.
- Enviar solicitudes a Compras por Kafka y mantener una vista de consulta del estado de las operaciones asíncronas a partir de los eventos recibidos.
- Exponer una API interna para consultar necesidades y ofertas, crear borradores, consultar órdenes y registrar decisiones locales.

## Interfaces

| Interlocutor | Canal | Propósito |
|---|---|---|
| `smartstock-front` | GraphQL | Consultas y acciones de la interfaz de SmartStock. |
| `smartstock-inventario` y `smartstock-ventas` | gRPC | Consultar stock, registrar ventas y consultarlas por identificador. |
| `smartstock-compras` | Kafka | Enviar solicitudes de compra y recibir sus resultados. |

## Límites y desarrollo

Gateway conserva un registro propio de solicitudes y resultados de Compras. Inventario, Ventas y Compras conservan la responsabilidad por sus datos y validaciones de negocio. Los contratos entre servicios y el despliegue conjunto se mantienen en `smartstock-infra`.

## Primer bloque: GraphQL y gRPC

El contrato efectivo es `vendor/smartstock-infra/contracts/graphql/schema.graphql`, de la revisión `c5867a8d48383dcadae2111eb2bf20770645e565`. También se cargan los `.proto` y esquemas Kafka de esa revisión en tiempo de ejecución. Antes de instalar o construir la imagen:

```sh
git submodule update --init --recursive
git -C vendor/smartstock-infra rev-parse HEAD
npm ci
npm test
npm run build
npm start
```

Requiere Node.js 22. GraphQL escucha en `http://localhost:3000/graphql`. Configuración opcional:

| Variable | Valor predeterminado | Descripción |
|---|---|---|
| `PORT` | `3000` | Puerto HTTP |
| `INVENTORY_GRPC_ADDRESS` | `localhost:50051` | Dirección gRPC de Inventario |
| `SALES_GRPC_ADDRESS` | `localhost:50052` | Dirección gRPC de Ventas |
| `INVENTORY_GRPC_TIMEOUT_MS` | `2000` | Plazo por llamada a Inventario |
| `SALES_GRPC_TIMEOUT_MS` | `2000` | Plazo por llamada a Ventas |

Las conexiones gRPC son internas y sin TLS; se necesita una red interna confiable o protección de transporte externa. Los plazos y el puerto deben ser enteros positivos. Dentro de Docker, configure las direcciones con nombres accesibles desde el contenedor, no `localhost` si los servicios son externos:

```sh
docker build -t smartstock-gateway .
docker run --rm -p 3000:3000 \
  -e INVENTORY_GRPC_ADDRESS=inventory:50051 \
  -e SALES_GRPC_ADDRESS=sales:50052 \
  smartstock-gateway
```

El contenedor debe estar en la red que resuelve `inventory` y `sales`. El contexto de build debe incluir el submódulo inicializado.

Ejemplo para servicios reales con el contrato acordado:

```sh
curl -s http://localhost:3000/graphql -H 'content-type: application/json' \
  -d '{"query":"query($sku: String!) { product(sku: $sku) { sku name stock threshold } }","variables":{"sku":"SKU-1"}}'
curl -s http://localhost:3000/graphql -H 'content-type: application/json' \
  -d '{"query":"mutation($input: CreateSaleInput!) { createSale(input: $input) { outcome sale { saleId sku quantity status rejectionReason } } }","variables":{"input":{"saleId":"sale-1","sku":"SKU-1","quantity":1}}}'
curl -s http://localhost:3000/graphql -H 'content-type: application/json' \
  -d '{"query":"query($id: ID!) { sale(id: $id) { saleId sku quantity status rejectionReason } }","variables":{"id":"sale-1"}}'
```

El cliente debe generar y conservar el identificador único y los argumentos antes de enviar. `ACCEPTED` no confirma la venta: puede seguir `PENDING`. Ante `UNKNOWN`, un error o una desconexión, consultar o reintentar con el **mismo** identificador y argumentos; `null` al consultar tampoco demuestra que una solicitud concurrente no termine. Las consultas informan `INVALID_ARGUMENT` o `UPSTREAM_UNAVAILABLE` como errores GraphQL; ausencia se representa por `null`.

`npm test` inicia servidores gRPC dobles y un servidor HTTP real para comprobar el recorrido GraphQL → gRPC, además de pruebas de adaptadores y resolver. Esos dobles no prueban la implementación real, la idempotencia, la persistencia ni la reconciliación de Ventas/Inventario.

## Integración local con Compras

La comunicación usa `purchases.requests.v1` y `purchases.results.v1`. Al configurar
`KAFKA_BOOTSTRAP_SERVERS`, Gateway requiere `PURCHASES_DATABASE_URL` (PostgreSQL
propio) y `PURCHASES_INTERNAL_TOKEN` (mínimo 16 caracteres). Sin Kafka configurado,
GraphQL sigue disponible y las solicitudes de Compras responden 503.
`PURCHASES_KAFKA_GROUP_ID` permite cambiar el grupo del consumidor; el valor por
defecto es `smartstock-gateway-purchases`.

Las rutas internas requieren `Authorization: Bearer <PURCHASES_INTERNAL_TOKEN>`:

| Método y ruta | Uso |
|---|---|
| `POST /internal/purchases/requests` | Cuerpo `{ "type": "...", "payload": {...} }`. Devuelve 202 con `request_id`, `order_id` si corresponde, `status` y `result`. |
| `GET /internal/purchases/requests/:id` | Consulta persistida; 404 si Gateway no conoce la solicitud. |
| `POST /internal/purchases/requests/:id/retry` | Reenvía la solicitud pendiente sin cambiar su ID, fecha ni argumentos. |

Usar una cabecera `Idempotency-Key` estable (1–256 caracteres) y conservarla
antes de enviar. Una repetición con el mismo tipo y argumentos recupera la
solicitud original, incluso tras perder la respuesta HTTP. Otra combinación
devuelve 409 sin publicar. Sin esa cabecera cada POST crea una operación nueva:
no repetir así una creación o decisión incierta.

El tipo y payload siguen los seis mensajes de Infra: `get_need`, `list_needs`,
`list_offers`, `get_order`, `create_draft` y `decide_order`. Para `create_draft`,
Gateway genera y conserva `order_id` si no se proporciona. Gateway siempre
genera `request_id`. Los payloads con NUL se rechazan porque JSONB no lo admite.

```sh
curl http://localhost:3001/internal/purchases/requests \
  -H 'Authorization: Bearer smartstock-local-development-only' \
  -H 'Idempotency-Key: offers-example-001' \
  -H 'Content-Type: application/json' \
  -d '{"type":"list_offers","payload":{"sku":"SKU-001","limit":10,"cursor":null}}'
```

`PENDING` significa que todavía no hay resultado definitivo, no un rechazo.
Gateway guarda antes de publicar, reenvía pendientes cada cinco segundos y
persiste resultados antes de confirmar offsets. Las respuestas repetidas
deben ser idénticas y corresponder a la operación y argumentos originales.
Una incompatibilidad detiene el consumidor sin confirmar el mensaje y requiere
investigación y reinicio después de resolverla. No borrar el registro para
forzar un nuevo intento.

Las decisiones están bloqueadas por defecto. La demostración local exige en
ambos servicios `PURCHASES_ENVIRONMENT=local`,
`PURCHASES_ALLOW_LOCAL_DECISIONS=true` y el actor configurado mediante
`PURCHASES_LOCAL_ACTOR_ID` (predeterminado `local-operator`). Este mecanismo,
el token de desarrollo y Kafka plaintext solo sirven en una red local de
confianza: no sustituyen autenticación de operadores, TLS y ACL de producción.

`npm test` incluye validación y autorización. Para ejecutar también las pruebas
reales del registro PostgreSQL, usar una base desechable dedicada:
`TEST_PURCHASES_DATABASE_URL=postgres://... npm test`. Crean tablas y solicitudes
con identificadores únicos sin borrar datos existentes.

## Integraciones pendientes

Las rutas de Compras son internas; todavía no se exponen operaciones de
reposición por GraphQL ni hay contrato acordado con MCP. No se accede a las
bases de otros servicios.
