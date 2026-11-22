# SmartStock · API Gateway

Backend for Frontend de SmartStock. Expone una API GraphQL para la interfaz, reúne información de los tres microservicios y adapta sus protocolos internos a las consultas y acciones del operador.

**Stack** · TypeScript, NestJS, Apollo Server, gRPC, Kafka y PostgreSQL.

## Responsabilidades

- Exponer a la interfaz consultas de stock, ventas y compras, junto con las acciones para registrar ventas, analizar reposiciones y decidir sobre un borrador.
- Componer respuestas que reúnen datos de más de un servicio y traducir errores a respuestas que pueda manejar el frontend.
- Enviar solicitudes a Compras por Kafka y mantener una vista de consulta del estado de las operaciones asíncronas a partir de los eventos recibidos.
- Iniciar análisis mediante la API REST interna del servicio MCP y ofrecer el acceso que sus herramientas necesitan para consultar datos y solicitar borradores.

## Interfaces

| Interlocutor | Canal | Propósito |
|---|---|---|
| `smartstock-front` | GraphQL | Consultas y acciones de la interfaz de SmartStock. |
| `smartstock-inventario` y `smartstock-ventas` | gRPC | Consultar stock e historial y registrar ventas. |
| `smartstock-compras` | Kafka | Enviar solicitudes de compra y recibir sus resultados. |
| `smartstock-mcp` | REST interno | Iniciar análisis y canalizar las consultas y acciones de las herramientas. |

## Límites y desarrollo

El Gateway mantiene en su PostgreSQL la vista de consulta que sirve al frontend. Inventario, Ventas y Compras conservan la responsabilidad por sus datos y validaciones de negocio. El repositorio alberga el esquema GraphQL, los resolvers, los clientes de integración y sus pruebas. Los contratos entre servicios y el despliegue conjunto se mantienen en `smartstock-infra`.
