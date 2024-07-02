## Usage example

```

let syncQueue = global.syncQueue;

export async function getQueue() {
  if (syncQueue) return syncQueue;

  let db = await getDb("swaplanet");

  const newQueue = QueueMongo({
    mongo: db,
    queueCollection: "queue",
    processEveryMs: 15000,
    maxTries: 3,
    delayBetweenJobs: 1000,
    jobDefinitions: {
      syncOrder,
      syncCustomer,
      syncProduct,
    },
    onError: async (e, errorType) => {
      const type =
        errorType == "queue-start"
          ? "SyncQueueStartError"
          : errorType == "queue-add"
            ? "SyncQueueAddError"
            : "SyncQueueError";
      let moment = new Moment();
      await logAction({
        type,
        requestedAt: moment.format(),
        error: e?.["message"],
        trace: e?.["stack"],
      });
    },
  });
  global.syncQueue = newQueue;

  newQueue.startQueue();

  return newQueue;
}

```
