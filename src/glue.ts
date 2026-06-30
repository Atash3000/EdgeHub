import { GlueClient, BatchCreatePartitionCommand } from "@aws-sdk/client-glue";

export function partitionValues(date: string): [string, string, string] {
  const [y, m, d] = date.split("-") as [string, string, string];
  return [y, m, d];
}

export async function addPartition(glue: GlueClient, database: string, table: string, bucket: string, prefix: string, date: string): Promise<void> {
  const [year, month, day] = partitionValues(date);
  const location = `s3://${bucket}/${prefix}/year=${year}/month=${month}/day=${day}/`;
  try {
    await glue.send(new BatchCreatePartitionCommand({
      DatabaseName: database, TableName: table,
      PartitionInputList: [{ Values: [year, month, day], StorageDescriptor: { Location: location } }],
    }));
  } catch (err) {
    if (((err as { name?: string }).name ?? "").includes("AlreadyExists")) return; // idempotent
    throw err;
  }
}
