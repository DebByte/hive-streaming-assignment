// ============================================================
// Receives telemetry batches from HiveStreamingCollector
// Writes to ADLS Gen2 raw/ container partitioned by eventDate
// ============================================================

import { app, HttpRequest, HttpResponseInit, InvocationContext }
  from "@azure/functions";
import { BlobServiceClient }      from "@azure/storage-blob";
import { DefaultAzureCredential } from "@azure/identity";
import { v4 as uuidv4 }           from "uuid";

//Core handler function for incoming telemetry batches
async function telemetryHandler(
  req:     HttpRequest,
  context: InvocationContext
): Promise<HttpResponseInit> {

  context.log("Telemetry batch received from:", req.headers.get("origin"));

  // Parse body
  let body: Record<string, unknown>;
  try {
    body = await req.json() as Record<string, unknown>;
  } catch {
    return { status: 400, body: "Invalid JSON body" };
  }

  // Validate required fields
  const { clientId, contentId, customerId, events, sentAt } = body;

  if (!clientId || !contentId || !customerId) {
    return {
      status: 400,
      body:   "Missing clientId, contentId or customerId"
    };
  }

  if (!events || !Array.isArray(events) || events.length === 0) {
    return { status: 400, body: "Missing or empty events array" };
  }

  //Add server-side metadata
  const receivedAt = new Date();
  const eventDate  = receivedAt.toISOString().split("T")[0]; // "2025-11-13"

  const enrichedPayload = {
    clientId,
    contentId,
    customerId,
    events,
    batchSize:        events.length,
    clientSentAt:     sentAt,
    serverReceivedAt: receivedAt.toISOString(),
    eventDate,
    schemaVersion:    "1.0"
  };

  // Write to ADLS Gen2 raw/ container
  // Hive partition format: raw/eventDate=2025-11-13/part-{uuid}.json
  try {
    const storageAccount = process.env["STORAGE_ACCOUNT_NAME"];

    if (!storageAccount) {
      throw new Error("STORAGE_ACCOUNT_NAME environment variable not set");
    }

    // DefaultAzureCredential:
    const credential = new DefaultAzureCredential();
    const accountUrl = `https://${storageAccount}.blob.core.windows.net`;

    const blobPath = `eventDate=${eventDate}/part-${uuidv4()}.json`;

    const blobClient = new BlobServiceClient(accountUrl, credential)
      .getContainerClient("raw")
      .getBlockBlobClient(blobPath);

    const content       = JSON.stringify(enrichedPayload, null, 2);
    const contentBuffer = Buffer.from(content);

    await blobClient.upload(contentBuffer, contentBuffer.length, {
      blobHTTPHeaders: { blobContentType: "application/json" }
    });

    context.log(`✅ Written: raw/${blobPath}`);

    return {
      status: 200,
      jsonBody: {
        message:  "OK",
        received: events.length,
        path:     `raw/${blobPath}`
      }
    };

  } catch (error) {
    context.error("Storage write failed:", error);
    return { status: 500, body: "Internal storage error" };
  }
}

// ── Register the HTTP trigger ──────────────────────────────
app.http("telemetry", {
  methods:   ["POST"],
  authLevel: "function",    // requires function key in header
  handler:   telemetryHandler
});