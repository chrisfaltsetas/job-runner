const QueueMongo = ({
  mongo,
  queueCollection = "queueJobs",
  concurrency = 1,
  processEveryMs = 30 * 1000,
  delayBetweenJobs = 0,
  lockLifetimeMs = 10 * 60 * 1000,
  maxTries = 1,
  jobDefinitions = {},
  onError = (e, _) => {
    throw e;
  },
}) => {
  processEveryMs = Math.max(processEveryMs, 5000); // minimum processing every 5 seconds
  concurrency = Math.max(concurrency, 1); // minimum concurrency is 1
  lockLifetimeMs = Math.max(lockLifetimeMs, 0); // minimum lockLifetimeMs is 0
  delayBetweenJobs = Math.max(delayBetweenJobs, 0); // minimum delayBetweenJobs is 0
  maxTries = Math.max(maxTries, 1); // minimum maxTries is 1

  async function _runJob(job) {
    let isSuccessful;
    try {
      isSuccessful = await jobDefinitions[job.name](job);
    } catch (e) {
      isSuccessful = false;
    }
    return isSuccessful !== false;
  }

  async function _processJob(job) {
    const isSuccessful = await _runJob(job);
    if (isSuccessful) {
      await mongo.collection(queueCollection).updateOne(
        { _id: job._id },
        {
          $set: {
            lockedAt: null,
            nextRunAt: null,
            lastFinishedAt: new Date(),
          },
        }
      );
    }
  }

  async function startQueue() {
    try {
      const nextJobs = [];
      for (let i = 0; i < concurrency; i++) {
        const { value: job } = await mongo
          .collection(queueCollection)
          .findOneAndUpdate(
            {
              nextRunAt: { $ne: null },
              $or: [
                { lockedAt: null },
                {
                  lockedAt: {
                    $lt: new Date(Date.now().valueOf() - lockLifetimeMs),
                  },
                },
              ],
              tries: { $lt: maxTries },
              lastFinishedAt: null,
            },
            {
              $set: {
                lockedAt: new Date(),
              },
              $inc: { tries: 1 },
            },
            {
              sort: { priority: -1, lockedAt: 1, nextRunAt: 1 },
              returnDocument: "after",
              returnNewDocument: true,
            }
          );
        if (job) nextJobs.push(job);
      }

      await Promise.all(
        nextJobs.map(async (job) => {
          await _processJob(job);
        })
      );

      const timeout =
        nextJobs.length < concurrency ? processEveryMs : delayBetweenJobs;
      setTimeout(startQueue, timeout);
    } catch (e) {
      onError(e, "queue-start");
    }
  }

  async function addToQueue(jobName, data, options = {}) {
    try {
      await mongo.collection(queueCollection).insertOne({
        name: jobName,
        data,
        tries: options?.maxTries ? maxTries - options.maxTries : 0,
        priority: options?.priority ?? 0,
        lockedAt: null,
        nextRunAt: new Date(),
        lastFinishedAt: null,
        createdAt: new Date(),
      });
      return true;
    } catch (e) {
      onError(e, "queue-add");
      return false;
    }
  }

  return { startQueue, addToQueue };
};

export default QueueMongo;
