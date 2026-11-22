# SmartStock · API Gateway

Backend for Frontend de SmartStock. Expone una API GraphQL para la interfaz y coordina el acceso a Inventario, Ventas, Compras y al servicio de IA.

**Stack** · TypeScript, NestJS, Apollo Server, gRPC, Kafka y PostgreSQL.

## Integraciones

| Componente | Protocolo | Operación |
|---|---|---|
| Frontend | GraphQL | Consultar stock, registrar ventas e interactuar con la reposición. |
| Inventario y Ventas | gRPC | Consultar existencias e historial, y enviar ventas a Ventas. |
| Compras | Kafka | Publicar comandos de compra y consumir actualizaciones de estado. |
| Servicio MCP | REST interno | Iniciar el análisis de reposición y exponer los datos y acciones que usan las herramientas. |

## Alcance del MVP

La API debe permitir registrar una venta, ver el saldo actualizado, iniciar el análisis de una alerta y consultar y aprobar el borrador resultante. En las operaciones asíncronas, la interfaz puede mostrar un estado pendiente hasta recibir el resultado de Compras.

El Gateway compone las respuestas para la interfaz y mantiene en PostgreSQL una vista de consulta del estado de las órdenes. Cada microservicio valida y persiste sus propias operaciones.

## Primera verificación

Enviar una mutación GraphQL de venta y comprobar que el Gateway la entrega a Ventas por gRPC. Los contratos de integración se mantienen en `smartstock-infra/contracts/`.
