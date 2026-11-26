function positiveInteger(name: string, fallback: number): number {
  const value = process.env[name] ?? String(fallback);
  if (!/^[0-9]+$/.test(value) || !Number.isSafeInteger(Number(value)) || Number(value) <= 0) {
    throw new Error(`${name} must be a positive integer`);
  }
  return Number(value);
}

export const config = {
  port: positiveInteger('PORT', 3000),
  inventoryAddress: process.env.INVENTORY_GRPC_ADDRESS || 'localhost:50051',
  salesAddress: process.env.SALES_GRPC_ADDRESS || 'localhost:50052',
  inventoryTimeoutMs: positiveInteger('INVENTORY_GRPC_TIMEOUT_MS', 2000),
  salesTimeoutMs: positiveInteger('SALES_GRPC_TIMEOUT_MS', 2000),
};
