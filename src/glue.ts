import { GlueClient, BatchCreatePartitionCommand, GetTableCommand } from "@aws-sdk/client-glue";

export function partitionValues(date: string): [string, string, string] {
  const [y, m, d] = date.split("-") as [string, string, string];
  return [y, m, d];
}

export async function addPartition(glue: GlueClient, database: string, table: string, bucket: string, prefix: string, date: string): Promise<void> {
  const [year, month, day] = partitionValues(date);
  const location = `s3://${bucket}/${prefix}/year=${year}/month=${month}/day=${day}/`;

  // Partitions must carry the table's columns + SerDe/format to be queryable; clone them and override Location only.
  const tbl = await glue.send(new GetTableCommand({ DatabaseName: database, Name: table }));
  const storageDescriptor = { ...(tbl.Table?.StorageDescriptor ?? {}), Location: location };

  try {
    await glue.send(new BatchCreatePartitionCommand({
      DatabaseName: database, TableName: table,
      PartitionInputList: [{ Values: [year, month, day], StorageDescriptor: storageDescriptor }],
    }));
  } catch (err) {
    if (((err as { name?: string }).name ?? "").includes("AlreadyExists")) return; // idempotent
    throw err;
  }
}

export async function addAsOfPartition(glue: GlueClient, database: string, table: string, bucket: string, prefix: string, asOf: string): Promise<void> {
  const location = `s3://${bucket}/${prefix}/asOf=${asOf}/`;
  const tbl = await glue.send(new GetTableCommand({ DatabaseName: database, Name: table }));
  const storageDescriptor = { ...(tbl.Table?.StorageDescriptor ?? {}), Location: location };
  try {
    await glue.send(new BatchCreatePartitionCommand({
      DatabaseName: database, TableName: table,
      PartitionInputList: [{ Values: [asOf], StorageDescriptor: storageDescriptor }],
    }));
  } catch (err) {
    if (((err as { name?: string }).name ?? "").includes("AlreadyExists")) return;
    throw err;
  }
}
