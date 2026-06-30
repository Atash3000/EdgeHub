import { describe, it, expect } from "vitest";
import { partitionValues, addPartition, addAsOfPartition } from "../src/glue.js";
import { GetTableCommand, BatchCreatePartitionCommand } from "@aws-sdk/client-glue";
import type { GlueClient } from "@aws-sdk/client-glue";

describe("partitionValues", () => {
  it("splits a date", () => { expect(partitionValues("2026-06-29")).toEqual(["2026", "06", "29"]); });
});

describe("addPartition", () => {
  const fakeSD = {
    Columns: [{ Name: "ticker", Type: "string" }],
    InputFormat: "IF",
    OutputFormat: "OF",
    SerdeInfo: { SerializationLibrary: "SERDE" },
    Location: "s3://old/loc/",
  };

  function makeClient(onBatch?: (input: unknown) => void) {
    const calls: string[] = [];
    let batchInput: unknown = null;
    const client = {
      send: async (cmd: unknown) => {
        if (cmd instanceof GetTableCommand) {
          calls.push("GetTable");
          return { Table: { StorageDescriptor: fakeSD } };
        }
        if (cmd instanceof BatchCreatePartitionCommand) {
          calls.push("BatchCreate");
          batchInput = (cmd as BatchCreatePartitionCommand).input;
          if (onBatch) onBatch(batchInput);
          return {};
        }
        throw new Error("unexpected command");
      },
      getCalls: () => calls,
      getBatchInput: () => batchInput,
    };
    return client;
  }

  it("calls GetTable before BatchCreatePartition, overrides Location, inherits SD fields", async () => {
    const client = makeClient();
    await addPartition(client as unknown as GlueClient, "mydb", "mytable", "bucket", "prefix", "2026-06-29");

    const calls = client.getCalls();
    expect(calls[0]).toBe("GetTable");
    expect(calls[1]).toBe("BatchCreate");

    const input = client.getBatchInput() as { PartitionInputList: Array<{ StorageDescriptor: { Location: string; InputFormat: string; Columns: Array<unknown> } }> };
    const sd = input.PartitionInputList[0]!.StorageDescriptor;
    expect(sd.Location).toBe("s3://bucket/prefix/year=2026/month=06/day=29/");
    expect(sd.InputFormat).toBe("IF");
    expect(sd.Columns).toHaveLength(1);
  });

  it("swallows AlreadyExistsException", async () => {
    const client = {
      send: async (cmd: unknown) => {
        if (cmd instanceof GetTableCommand) return { Table: { StorageDescriptor: fakeSD } };
        const err = Object.assign(new Error("already"), { name: "AlreadyExistsException" });
        throw err;
      },
    };
    await expect(addPartition(client as unknown as GlueClient, "db", "t", "b", "p", "2026-06-29")).resolves.toBeUndefined();
  });

  it("rethrows non-AlreadyExists errors", async () => {
    const client = {
      send: async (cmd: unknown) => {
        if (cmd instanceof GetTableCommand) return { Table: { StorageDescriptor: fakeSD } };
        const err = Object.assign(new Error("boom"), { name: "InternalServiceException" });
        throw err;
      },
    };
    await expect(addPartition(client as unknown as GlueClient, "db", "t", "b", "p", "2026-06-29")).rejects.toMatchObject({ name: "InternalServiceException" });
  });
});

describe("addAsOfPartition", () => {
  it("registers a single asOf partition with the asOf location", async () => {
    let created: { Values: string[]; StorageDescriptor: { Location: string } } | undefined;
    const glue = {
      send: async (c: any) => {
        if (c.constructor.name === "GetTableCommand") return { Table: { StorageDescriptor: { Columns: [], SerdeInfo: {} } } };
        if (c.constructor.name === "BatchCreatePartitionCommand") created = c.input.PartitionInputList[0];
        return {};
      },
    } as never;
    await addAsOfPartition(glue, "edgehub", "securities", "bkt", "reference/securities", "2026-06-30");
    expect(created!.Values).toEqual(["2026-06-30"]);
    expect(created!.StorageDescriptor.Location).toBe("s3://bkt/reference/securities/asOf=2026-06-30/");
  });
});
