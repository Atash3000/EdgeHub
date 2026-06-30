import { SecretsManagerClient, GetSecretValueCommand } from "@aws-sdk/client-secrets-manager";

export function parseSecret(json: string): Record<string, string> {
  const obj = JSON.parse(json);
  if (typeof obj !== "object" || obj === null) throw new Error("secret is not an object");
  return obj as Record<string, string>;
}

export async function loadSecrets(client: SecretsManagerClient, secretName: string): Promise<Record<string, string>> {
  const res = await client.send(new GetSecretValueCommand({ SecretId: secretName }));
  if (!res.SecretString) throw new Error("secret has no SecretString");
  return parseSecret(res.SecretString);
}
